package dmap

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/internal/cluster/partitions"
	"github.com/voluzi/olric/internal/testcluster"
	"github.com/voluzi/olric/internal/testutil"
	"github.com/voluzi/olric/pkg/storage"
)

type maintenanceEngine struct {
	storage.Engine
	statsEntered, releaseStats chan struct{}
	compactions, retiredReads  atomic.Int64
	retired                    atomic.Bool
}

func (e *maintenanceEngine) Stats() storage.Stats {
	if e.statsEntered != nil {
		close(e.statsEntered)
		<-e.releaseStats
	}
	return e.Engine.Stats()
}
func (e *maintenanceEngine) Compaction() (bool, error) {
	e.compactions.Add(1)
	return e.Engine.Compaction()
}
func (e *maintenanceEngine) Close() error { e.retired.Store(true); return e.Engine.Close() }
func (e *maintenanceEngine) RangeHKey(fn func(uint64) bool) {
	if e.retired.Load() {
		e.retiredReads.Add(1)
	}
	e.Engine.RangeHKey(fn)
}

func TestDMapMaintenanceContinuesAfterJanitorRetiresFragment(t *testing.T) {
	for _, operation := range []string{"compaction", "eviction"} {
		t.Run(operation, func(t *testing.T) {
			cfg := testutil.NewConfig()
			cfg.DMaps.CheckEmptyFragmentsInterval = time.Hour
			cfg.DMaps.TriggerCompactionInterval = time.Hour
			cluster := testcluster.New(NewService)
			s := cluster.AddMember(testcluster.NewEnvironment(cfg)).(*Service)
			defer cluster.Shutdown()
			dm, err := s.NewDMap("retired")
			require.NoError(t, err)
			f, err := dm.newFragment()
			require.NoError(t, err)
			engine := &maintenanceEngine{Engine: f.storage, statsEntered: make(chan struct{}), releaseStats: make(chan struct{})}
			f.storage = engine
			part := s.primary.PartitionByID(0)
			part.Map().Store(dm.fragmentName, f)
			janitorDone := make(chan struct{})
			go func() { s.janitor(part); close(janitorDone) }()
			<-engine.statsEntered
			done := make(chan struct{})
			go func() {
				if operation == "compaction" {
					s.callCompactionOnFragment(f)
				} else {
					s.scanFragmentForEviction(0, dm.name, f)
				}
				close(done)
			}()
			// Hold the janitor inside Stats while maintenance races with retirement.
			time.Sleep(20 * time.Millisecond)
			close(engine.releaseStats)
			<-janitorDone
			select {
			case <-done:
			case <-time.After(time.Second):
				s.cancel()
				<-done
				t.Fatal("maintenance stayed on the retired fragment")
			}
			require.Zero(t, engine.compactions.Load(), "retired storage must not be compacted")
			require.Zero(t, engine.retiredReads.Load(), "retired storage must not be scanned")

			live, err := s.NewDMap("live")
			require.NoError(t, err)
			require.NoError(t, live.Put(context.Background(), "key", []byte("value"), nil))
			livePart := live.getPartitionByHKey(partitions.HKey(live.name, "key"), partitions.PRIMARY)
			liveFragment, err := live.loadFragment(livePart)
			require.NoError(t, err)
			observed := &maintenanceEngine{Engine: liveFragment.storage}
			liveFragment.Lock()
			liveFragment.storage = observed
			liveFragment.Unlock()
			s.triggerCompaction()
			s.triggerCompaction()
			require.Equal(t, int64(2), observed.compactions.Load(), "later passes must reach live storage")
		})
	}
}
