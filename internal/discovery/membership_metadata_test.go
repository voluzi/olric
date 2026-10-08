// Copyright 2018-2025 The Olric Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discovery

import (
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/internal/testutil"
)

func requireNativeMembership(t *testing.T, d *Discovery, expected []Member) {
	t.Helper()
	var nativeNames, cachedNames []string
	// Node.Name is immutable; Node.Meta can change after Members() returns.
	for _, node := range d.memberlist.Members() {
		nativeNames = append(nativeNames, node.Name)
	}
	members := d.GetMembers()
	for _, member := range members {
		cachedNames = append(cachedNames, member.Name)
	}
	sort.Strings(nativeNames)
	sort.Strings(cachedNames)
	require.Equal(t, nativeNames, cachedNames)
	require.Equal(t, len(nativeNames), d.NumMembers())
	require.ElementsMatch(t, expected, members)
}

func TestDiscoveryLocalMemberPresentFromStart(t *testing.T) {
	cfg := testutil.NewConfig()
	d := New(testutil.NewFlogger(cfg), cfg)
	require.NoError(t, d.Start())
	t.Cleanup(func() { require.NoError(t, d.Shutdown()) })
	requireNativeMembership(t, d, []Member{*d.member})
	require.Equal(t, *d.member, d.GetCoordinator())
}

func TestDiscoveryMembershipReadsDuringAliveUpdates(t *testing.T) {
	cluster := newTestCluster(t)
	first := cluster.addNewMember(t)
	second := cluster.addNewMember(t)
	expected := []Member{*first.member, *second.member}
	require.Eventually(t, func() bool { return len(first.GetMembers()) == 2 }, time.Second, time.Millisecond)
	requireNativeMembership(t, first, expected)
	requireNativeMembership(t, second, expected)

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for _, discovery := range []*Discovery{first, second} {
		readers.Add(1)
		go func(d *Discovery) {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					members := d.GetMembers()
					if len(members) != 2 || members[0] != expected[0] || members[1] != expected[1] {
						t.Errorf("unexpected live membership during alive update: %v", members)
						return
					}
					if member, err := d.FindMemberByName(second.member.Name); err != nil || member != expected[1] {
						t.Errorf("member lookup during alive update: %v, %v", member, err)
						return
					}
					if d.GetCoordinator() != expected[0] {
						t.Error("alive update changed coordinator")
						return
					}
				}
			}
		}(discovery)
	}
	t.Cleanup(func() { close(stop); readers.Wait() })
	for i := 0; i < 20; i++ {
		// UpdateNode rewrites native metadata even when its bytes are unchanged.
		require.NoError(t, second.memberlist.UpdateNode(2*time.Second))
	}
}

func TestDiscoverySameNameRejoinReplacesIdentity(t *testing.T) {
	cluster := newTestCluster(t)
	first := cluster.addNewMember(t)
	cfg := testutil.NewConfig()
	cfg.Peers = []string{cluster.members[0]}
	second := New(testutil.NewFlogger(cfg), cfg)
	require.NoError(t, second.Start())
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			require.NoError(t, second.Shutdown())
		}
	})
	_, err := second.Join()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		member, err := first.FindMemberByName(second.member.Name)
		return err == nil && member == *second.member
	}, 2*time.Second, time.Millisecond)
	requireNativeMembership(t, first, []Member{*first.member, *second.member})

	require.NoError(t, second.Shutdown())
	stopped = true
	require.Eventually(t, func() bool {
		_, err := first.FindMemberByID(second.member.ID)
		return err == ErrMemberNotFound && len(first.GetMembers()) == 1
	}, 2*time.Second, time.Millisecond)
	requireNativeMembership(t, first, []Member{*first.member})

	nextConfig := testutil.NewConfig()
	nextConfig.MemberlistConfig.Name = second.member.Name
	nextConfig.Peers = []string{cluster.members[0]}
	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	for port == cfg.MemberlistConfig.BindPort {
		port, err = testutil.GetFreePort()
		require.NoError(t, err)
	}
	nextConfig.MemberlistConfig.BindPort = port
	nextConfig.MemberlistConfig.AdvertisePort = port
	next := New(testutil.NewFlogger(nextConfig), nextConfig)
	require.NoError(t, next.Start())
	t.Cleanup(func() { require.NoError(t, next.Shutdown()) })
	_, err = next.Join()
	require.NoError(t, err)
	require.NotEqual(t, second.member.ID, next.member.ID)
	require.NotEqual(t, cfg.MemberlistConfig.BindPort, nextConfig.MemberlistConfig.BindPort)
	require.Eventually(t, func() bool {
		member, err := first.FindMemberByName(second.member.Name)
		return err == nil && member == *next.member
	}, 2*time.Second, time.Millisecond)
	_, err = first.FindMemberByID(second.member.ID)
	require.ErrorIs(t, err, ErrMemberNotFound)
	requireNativeMembership(t, first, []Member{*first.member, *next.member})
	requireNativeMembership(t, next, []Member{*first.member, *next.member})
	require.Equal(t, *first.member, first.GetCoordinator())
}

type updatingMetadata struct {
	delegate
	metadata atomic.Value
}

func (d *updatingMetadata) NodeMeta(int) []byte {
	return d.metadata.Load().([]byte)
}

func TestDiscoveryMembershipMetadataUpdateReplacesIdentity(t *testing.T) {
	cluster := newTestCluster(t)
	observer := cluster.addNewMember(t)
	cfg := testutil.NewConfig()
	original := NewMember(cfg)
	encoded, err := original.Encode()
	require.NoError(t, err)
	metadata := &updatingMetadata{}
	metadata.metadata.Store(encoded)
	cfg.MemberlistConfig.Delegate = metadata
	publisher, err := memberlist.Create(cfg.MemberlistConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, publisher.Leave(time.Second))
		require.NoError(t, publisher.Shutdown())
	})
	_, err = publisher.Join([]string{cluster.members[0]})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		member, err := observer.FindMemberByName(original.Name)
		return err == nil && member == original
	}, 2*time.Second, time.Millisecond)

	requireNativeMembership(t, observer, []Member{*observer.member, original})
	stop := make(chan struct{})
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for {
			select {
			case <-stop:
				return
			default:
				members := observer.GetMembers()
				if len(members) != 2 || members[0] != *observer.member || members[1].Name != original.Name || members[1].ID != MemberID(original.Name, members[1].Birthdate) {
					t.Errorf("inconsistent membership during metadata events: %v", members)
					return
				}
			}
		}
	}()
	t.Cleanup(func() { close(stop); reader.Wait() })
	var updated Member
	for i := 0; i < 20; i++ {
		updated = NewMember(cfg)
		require.NotEqual(t, original.ID, updated.ID)
		encoded, err = updated.Encode()
		require.NoError(t, err)
		metadata.metadata.Store(encoded)
		require.NoError(t, publisher.UpdateNode(2*time.Second))
	}

	require.Eventually(t, func() bool {
		member, err := observer.FindMemberByName(original.Name)
		return err == nil && member == updated
	}, 2*time.Second, time.Millisecond)
	_, err = observer.FindMemberByID(original.ID)
	require.ErrorIs(t, err, ErrMemberNotFound)
	requireNativeMembership(t, observer, []Member{*observer.member, updated})
	require.Equal(t, *observer.member, observer.GetCoordinator())
}

func TestDiscoveryDeadMemberWithoutLeaveMatchesNativeView(t *testing.T) {
	cfg := testutil.NewConfig()
	cfg.MemberlistConfig.ProbeInterval = 100 * time.Millisecond
	cfg.MemberlistConfig.ProbeTimeout = 50 * time.Millisecond
	cfg.MemberlistConfig.SuspicionMult = 1
	cfg.MemberlistConfig.SuspicionMaxTimeoutMult = 1
	cfg.MemberlistConfig.DisableTcpPings = true
	cfg.MemberlistConfig.IndirectChecks = 0
	cfg.MemberlistConfig.GossipInterval = 20 * time.Millisecond
	observer := New(testutil.NewFlogger(cfg), cfg)
	require.NoError(t, observer.Start())
	t.Cleanup(func() { require.NoError(t, observer.Shutdown()) })
	requireNativeMembership(t, observer, []Member{*observer.member})

	remoteConfig := testutil.NewConfig()
	remote := NewMember(remoteConfig)
	encoded, err := remote.Encode()
	require.NoError(t, err)
	remoteConfig.MemberlistConfig.Delegate = delegate{meta: encoded}
	publisher, err := memberlist.Create(remoteConfig.MemberlistConfig)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, publisher.Shutdown()) })
	peer := net.JoinHostPort(cfg.MemberlistConfig.BindAddr, strconv.Itoa(cfg.MemberlistConfig.BindPort))
	_, err = publisher.Join([]string{peer})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(observer.GetMembers()) == 2 && observer.memberlist.NumMembers() == 2
	}, 2*time.Second, time.Millisecond)
	requireNativeMembership(t, observer, []Member{*observer.member, remote})

	// Shutdown closes gossip without sending Leave; the observer must detect death.
	require.NoError(t, publisher.Shutdown())
	require.Eventually(t, func() bool {
		return observer.memberlist.NumMembers() == 1 && len(observer.GetMembers()) == 1
	}, 5*time.Second, 10*time.Millisecond)
	_, err = observer.FindMemberByID(remote.ID)
	require.ErrorIs(t, err, ErrMemberNotFound)
	requireNativeMembership(t, observer, []Member{*observer.member})
	require.Equal(t, *observer.member, observer.GetCoordinator())
}
