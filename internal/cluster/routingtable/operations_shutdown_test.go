package routingtable

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/voluzi/olric/internal/protocol"
	"github.com/voluzi/olric/internal/testutil"
)

func TestRoutingTable_RejectUpdatesAfterShutdown(t *testing.T) {
	cluster := newTestCluster()
	t.Cleanup(func() { require.NoError(t, cluster.shutdown()) })
	rt, err := cluster.addNode(testutil.NewConfig())
	require.NoError(t, err)
	rt.RLock()
	payload, err := msgpack.Marshal(rt.table)
	rt.RUnlock()
	require.NoError(t, err)
	client := redis.NewClient(&redis.Options{Addr: rt.This().String()})
	defer client.Close()
	var callbacks atomic.Uint64
	rt.AddCallback(func() { callbacks.Add(1) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	update := func() error {
		cmd := protocol.NewUpdateRouting(payload, rt.This().ID).Command(ctx)
		return protocol.ConvertError(client.Process(ctx, cmd))
	}
	require.NoError(t, update())
	require.Eventually(t, func() bool { return callbacks.Load() > 0 }, time.Second, time.Millisecond)

	var workers sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = update()
				}
			}
		}()
	}
	err = rt.Shutdown(ctx)
	close(stop)
	workers.Wait()
	require.NoError(t, err)
	before := callbacks.Load()
	for i := 0; i < 10; i++ {
		require.ErrorIs(t, update(), ErrServerGone)
	}
	require.Equal(t, before, callbacks.Load())
}
