package dmap

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/internal/cluster/partitions"
	"github.com/voluzi/olric/internal/testcluster"
	"github.com/voluzi/olric/internal/testutil"
	"github.com/voluzi/olric/pkg/storage"
)

func TestDMapJanitorRetainsAcknowledgedWrites(t *testing.T) {
	cfg := testutil.NewConfig()
	cfg.DMaps.CheckEmptyFragmentsInterval = time.Microsecond
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(testcluster.NewEnvironment(cfg)).(*Service)
	defer cluster.Shutdown()
	dm, err := s.NewDMap("janitor")
	require.NoError(t, err)

	var wg sync.WaitGroup
	failures := make(chan error, 3200)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			key := fmt.Sprint("writer-", worker)
			for i := 0; i < 200; i++ {
				value := []byte(fmt.Sprint(i))
				if err := dm.Put(context.Background(), key, value, nil); err != nil {
					failures <- fmt.Errorf("put: %w", err)
					continue
				}
				entry, err := dm.Get(context.Background(), key)
				if err != nil {
					failures <- fmt.Errorf("acknowledged put disappeared: %w", err)
				} else if !bytes.Equal(entry.Value(), value) {
					failures <- fmt.Errorf("got %q, want %q", entry.Value(), value)
				}
				if _, err := dm.Delete(context.Background(), key); err != nil {
					failures <- fmt.Errorf("delete: %w", err)
				}
			}
		}(worker)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
}

type shutdownReadEngine struct {
	storage.Engine
	cancel context.CancelFunc
}

func (e shutdownReadEngine) Get(hkey uint64) (storage.Entry, error) {
	entry, err := e.Engine.Get(hkey)
	e.cancel()
	return entry, err
}

func TestDMapGetRetainsReadDuringShutdown(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()
	dm, err := s.NewDMap("shutdown")
	require.NoError(t, err)
	require.NoError(t, dm.Put(context.Background(), "key", []byte("value"), nil))
	part := dm.getPartitionByHKey(partitions.HKey(dm.name, "key"), partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	require.NoError(t, err)
	f.Lock()
	// Cancel after the storage read, before the idle check.
	f.storage = shutdownReadEngine{Engine: f.storage, cancel: s.cancel}
	f.Unlock()
	entry, err := dm.Get(context.Background(), "key")
	require.NoError(t, err)
	require.Equal(t, []byte("value"), entry.Value())
}
