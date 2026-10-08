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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/internal/testutil"
)

func TestDiscoveryMembershipReadsDuringAliveUpdates(t *testing.T) {
	cluster := newTestCluster(t)
	first := cluster.addNewMember(t)
	second := cluster.addNewMember(t)
	expected := []Member{*first.member, *second.member}
	require.Eventually(t, func() bool { return len(first.GetMembers()) == 2 }, time.Second, time.Millisecond)

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

	require.NoError(t, second.Shutdown())
	stopped = true
	require.Eventually(t, func() bool {
		_, err := first.FindMemberByID(second.member.ID)
		return err == ErrMemberNotFound && len(first.GetMembers()) == 1
	}, 2*time.Second, time.Millisecond)

	nextConfig := testutil.NewConfig()
	nextConfig.MemberlistConfig.Name = second.member.Name
	nextConfig.Peers = []string{cluster.members[0]}
	next := New(testutil.NewFlogger(nextConfig), nextConfig)
	require.NoError(t, next.Start())
	t.Cleanup(func() { require.NoError(t, next.Shutdown()) })
	_, err = next.Join()
	require.NoError(t, err)
	require.NotEqual(t, second.member.ID, next.member.ID)
	require.Eventually(t, func() bool {
		member, err := first.FindMemberByName(second.member.Name)
		return err == nil && member == *next.member
	}, 2*time.Second, time.Millisecond)
	_, err = first.FindMemberByID(second.member.ID)
	require.ErrorIs(t, err, ErrMemberNotFound)
	require.Equal(t, []Member{*first.member, *next.member}, first.GetMembers())
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

	updated := NewMember(cfg)
	require.NotEqual(t, original.ID, updated.ID)
	encoded, err = updated.Encode()
	require.NoError(t, err)
	metadata.metadata.Store(encoded)
	require.NoError(t, publisher.UpdateNode(2*time.Second))
	require.Eventually(t, func() bool {
		member, err := observer.FindMemberByName(original.Name)
		return err == nil && member == updated
	}, 2*time.Second, time.Millisecond)
	_, err = observer.FindMemberByID(original.ID)
	require.ErrorIs(t, err, ErrMemberNotFound)
	require.Equal(t, []Member{*observer.member, updated}, observer.GetMembers())
	require.Equal(t, *observer.member, observer.GetCoordinator())
}
