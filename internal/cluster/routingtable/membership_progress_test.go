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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/internal/testutil"
)

type pausedLengthConn struct {
	net.Conn
	enabled *atomic.Bool
	entered chan struct{}
	resume  chan struct{}
	once    *sync.Once
}

func (c pausedLengthConn) Write(data []byte) (int, error) {
	if c.enabled.Load() && bytes.Contains(data, []byte("internal.node.lengthofpart")) {
		c.once.Do(func() { close(c.entered) })
		<-c.resume
	}
	return c.Conn.Write(data)
}
func TestRoutingTableProcessesLeaveWhileRoutingScanWaits(t *testing.T) {
	cluster := newTestCluster()
	t.Cleanup(func() { require.NoError(t, cluster.shutdown()) })
	var enabled atomic.Bool
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c := testutil.NewConfig()
	c.ReplicaCount = 2
	base := c.Client.Dialer
	c.Client.Dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := base(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return pausedLengthConn{conn, &enabled, entered, resume, &once}, nil
	}
	coordinator, err := cluster.addNode(c)
	require.NoError(t, err)
	c = testutil.NewConfig()
	c.ReplicaCount = 2
	leaving, err := cluster.addNode(c)
	require.NoError(t, err)
	require.Eventually(t, leaving.IsBootstrapped, 5*time.Second, 10*time.Millisecond)
	coordinator.UpdateEagerly()
	enabled.Store(true)
	t.Cleanup(func() { enabled.Store(false); close(resume) })
	c = testutil.NewConfig()
	c.ReplicaCount = 2
	_, err = cluster.addNode(c)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("routing scan did not start")
	}
	require.NoError(t, leaving.Shutdown(context.Background()))
	require.Eventually(t, func() bool {
		coordinator.Members().RLock()
		defer coordinator.Members().RUnlock()
		_, err := coordinator.Members().Get(leaving.This().ID)
		return err != nil
	}, 2*time.Second, 10*time.Millisecond, "leave processing stayed queued behind routing RPCs")
}
