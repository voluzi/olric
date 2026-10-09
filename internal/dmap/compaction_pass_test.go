package dmap

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/internal/testcluster"
	"github.com/voluzi/olric/internal/testutil"
)

func TestDMapCompactionPassContinuesPastRetiredFragments(t *testing.T) {
	cfg := testutil.NewConfig()
	cfg.DMaps.CheckEmptyFragmentsInterval = time.Hour
	cfg.DMaps.TriggerCompactionInterval = time.Hour
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(testcluster.NewEnvironment(cfg)).(*Service)
	defer cluster.Shutdown()
	part := s.primary.PartitionByID(0)
	// Range can start with either map entry; repeat passes so both orders occur.
	for i := 0; i < 64; i++ {
		retired, err := s.NewDMap("retired-pass")
		require.NoError(t, err)
		live, err := s.NewDMap("live-pass")
		require.NoError(t, err)
		f, err := retired.newFragment()
		require.NoError(t, err)
		require.NoError(t, f.Close())
		other, err := live.newFragment()
		require.NoError(t, err)
		observed := &maintenanceEngine{Engine: other.storage}
		other.storage = observed
		part.Map().Store(retired.fragmentName, f)
		part.Map().Store(live.fragmentName, other)
		s.doCompaction(0)
		require.Equal(t, int64(1), observed.compactions.Load(), "the live fragment must be compacted in the same pass")
		part.Map().Delete(retired.fragmentName)
		part.Map().Delete(live.fragmentName)
		require.NoError(t, f.Destroy())
		require.NoError(t, other.Close())
		require.NoError(t, other.Destroy())
	}
}
