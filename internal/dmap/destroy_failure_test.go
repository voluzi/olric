package dmap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/internal/cluster/partitions"
	"github.com/voluzi/olric/internal/testcluster"
	"github.com/voluzi/olric/internal/testutil"
	"github.com/voluzi/olric/pkg/storage"
)

type failingRetirementEngine struct {
	storage.Engine
	closeErr, destroyErr error
}

func (e failingRetirementEngine) Close() error {
	if e.closeErr != nil {
		return e.closeErr
	}
	return e.Engine.Close()
}
func (e failingRetirementEngine) Destroy() error { return e.destroyErr }

func TestDMapRetirementFailureRemovesRetiredFragment(t *testing.T) {
	for _, operation := range []string{"Close", "Destroy"} {
		t.Run(operation, func(t *testing.T) {
			cfg := testutil.NewConfig()
			cfg.DMaps.CheckEmptyFragmentsInterval = time.Hour
			cfg.DMaps.TriggerCompactionInterval = time.Hour
			cluster := testcluster.New(NewService)
			s := cluster.AddMember(testcluster.NewEnvironment(cfg)).(*Service)
			defer cluster.Shutdown()
			dm, err := s.NewDMap("destroy-failure")
			require.NoError(t, err)
			require.NoError(t, dm.Put(context.Background(), "key", []byte("old"), nil))
			part := dm.getPartitionByHKey(partitions.HKey(dm.name, "key"), partitions.PRIMARY)
			f, err := dm.loadFragment(part)
			require.NoError(t, err)
			cause := errors.New(operation + " failed")
			f.Lock()
			underlying := f.storage
			broken := failingRetirementEngine{Engine: underlying}
			if operation == "Close" {
				broken.closeErr = cause
			} else {
				broken.destroyErr = cause
			}
			f.storage = broken
			err = wipeOutFragment(part, dm.fragmentName, f)
			f.Unlock()
			defer underlying.Destroy()
			defer underlying.Close()
			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, f.ctx.Err(), context.Canceled)
			_, mapped := part.Map().Load(dm.fragmentName)
			require.False(t, mapped, "a retired fragment must not remain mapped after retirement fails")
			require.NoError(t, dm.Put(context.Background(), "key", []byte("new"), nil))
			entry, err := dm.Get(context.Background(), "key")
			require.NoError(t, err)
			require.Equal(t, []byte("new"), entry.Value())
		})
	}
}
