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

type failingDestroyEngine struct {
	storage.Engine
	err error
}

func (e failingDestroyEngine) Destroy() error { return e.err }

func TestDMapDestroyFailureRemovesRetiredFragment(t *testing.T) {
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
	cause := errors.New("destroy failed")
	f.Lock()
	underlying := f.storage
	f.storage = failingDestroyEngine{underlying, cause}
	err = wipeOutFragment(part, dm.fragmentName, f)
	f.Unlock()
	defer underlying.Destroy()
	require.ErrorIs(t, err, cause)
	_, mapped := part.Map().Load(dm.fragmentName)
	require.False(t, mapped, "a retired fragment must not remain mapped after Destroy fails")
	require.NoError(t, dm.Put(context.Background(), "key", []byte("new"), nil))
	entry, err := dm.Get(context.Background(), "key")
	require.NoError(t, err)
	require.Equal(t, []byte("new"), entry.Value())
}
