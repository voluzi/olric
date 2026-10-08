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

package routingtable

import (
	"bytes"
	"context"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/internal/discovery"
	"github.com/voluzi/olric/internal/testutil"
	"github.com/voluzi/olric/internal/testutil/mockfragment"
)

type countedLengthConn struct {
	net.Conn
	enabled      *atomic.Bool
	active, peak *atomic.Int64
}

func (c countedLengthConn) Write(b []byte) (int, error) {
	if c.enabled.Load() && bytes.Contains(b, []byte("internal.node.lengthofpart")) {
		n := c.active.Add(1)
		for p := c.peak.Load(); n > p; p = c.peak.Load() {
			if c.peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		defer c.active.Add(-1)
	}
	return c.Conn.Write(b)
}

func TestRoutingTableParallelScanPreservesSerialOwners(t *testing.T) {
	cluster := newTestCluster()
	t.Cleanup(func() { require.NoError(t, cluster.shutdown()) })
	var enabled atomic.Bool
	var active, peak atomic.Int64
	var nodes []*RoutingTable
	for i := range 3 {
		c := testutil.NewConfig()
		c.PartitionCount = 31
		c.ReplicaCount = 2
		c.RoutingTablePushInterval = time.Hour
		if i == 0 {
			base := c.Client.Dialer
			c.Client.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := base(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return countedLengthConn{conn, &enabled, &active, &peak}, nil
			}
		}
		r, err := cluster.addNode(c)
		require.NoError(t, err)
		nodes = append(nodes, r)
		require.Eventually(t, r.IsBootstrapped, 5*time.Second, 10*time.Millisecond)
	}
	r := nodes[0]
	r.Lock()
	defer r.Unlock()
	for id := uint64(0); id < r.config.PartitionCount; id++ {
		for i, node := range nodes {
			if (id+uint64(i))%2 == 0 {
				f := mockfragment.New()
				f.Put("key", "value")
				node.primary.PartitionByID(id).Map().Store("data", f)
			}
			if (id+uint64(i))%3 == 0 {
				f := mockfragment.New()
				f.Put("key", "value")
				node.backup.PartitionByID(id).Map().Store("data", f)
			}
		}
		restarted := nodes[1].This()
		restarted.ID++
		owners := []discovery.Member{{Name: "departed", ID: 999}, restarted, nodes[0].This(), nodes[1].This(), nodes[2].This()}
		r.primary.PartitionByID(id).SetOwners(owners)
		r.backup.PartitionByID(id).SetOwners(owners)
	}
	expected := make(map[uint64]*route)
	for id := uint64(0); id < r.config.PartitionCount; id++ {
		expected[id] = &route{Owners: r.distributePrimaryCopies(id), Backups: r.distributeBackups(id)}
	}
	enabled.Store(true)
	r.fillRoutingTable()
	enabled.Store(false)
	if !reflect.DeepEqual(r.table, expected) {
		t.Fatal("parallel scan changed serial ownership lists")
	}
	if n := peak.Load(); n < 2 || n > 16 {
		t.Fatalf("ownership RPC fan-out: %d, want 2..16", n)
	}
}
