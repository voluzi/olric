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

package dmap

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/voluzi/olric/events"
	"github.com/voluzi/olric/internal/cluster/partitions"
	"github.com/voluzi/olric/internal/discovery"
	"github.com/voluzi/olric/internal/protocol"
	"github.com/voluzi/olric/pkg/storage"
	"github.com/vmihailenco/msgpack/v5"
)

type fragment struct {
	sync.RWMutex

	service *Service
	storage storage.Engine
	ctx     context.Context
	cancel  context.CancelFunc
}

func (f *fragment) Stats() storage.Stats {
	f.RLock()
	defer f.RUnlock()

	return f.storage.Stats()
}

func (f *fragment) Compaction() (bool, error) {
	select {
	case <-f.ctx.Done():
		// fragment is closed or destroyed
		return false, nil
	default:
	}
	return f.storage.Compaction()
}

func (f *fragment) Destroy() error {
	select {
	case <-f.ctx.Done():
		return f.storage.Destroy()
	default:
	}
	return errors.New("fragment is not closed")
}

func (f *fragment) Close() error {
	defer f.cancel()
	return f.storage.Close()
}

func (f *fragment) Name() string {
	return "DMap"
}

func (f *fragment) Move(part *partitions.Partition, name string, owners []discovery.Member) error {
	f.Lock()
	defer f.Unlock()

	i := f.storage.TransferIterator()
	if !i.Next() {
		return nil
	}

	payload, index, err := i.Export()
	if err != nil {
		return err
	}
	fp := &fragmentPack{
		PartID:  part.ID(),
		Kind:    part.Kind(),
		Name:    strings.TrimPrefix(name, "dmap."),
		Payload: payload,
	}
	value, err := msgpack.Marshal(fp)
	if err != nil {
		return err
	}

	for _, owner := range owners {
		if f.service.config.EnableClusterEventsChannel {
			e := &events.FragmentMigrationEvent{
				Kind:          events.KindFragmentMigrationEvent,
				Source:        f.service.rt.This().String(),
				Target:        owner.String(),
				DataStructure: "dmap",
				PartitionID:   part.ID(),
				Identifier:    fp.Name,
				Length:        len(value),
				IsBackup:      part.Kind() == partitions.BACKUP,
				Timestamp:     time.Now().UnixNano(),
			}
			f.service.wg.Add(1)
			go f.service.publishEvent(e)
		}

		cmd := protocol.NewMoveFragment(value).Command(f.service.ctx)
		rc := f.service.client.Get(owner.String())
		err = rc.Process(f.service.ctx, cmd)
		if err != nil {
			return err
		}
		if err := cmd.Err(); err != nil {
			return err
		}
	}

	return i.Drop(index)
}

func (dm *DMap) newFragment() (*fragment, error) {
	c := storage.NewConfig(dm.config.engine.Config)
	engine, err := dm.engine.Fork(c)
	if err != nil {
		return nil, err
	}

	engine.SetLogger(dm.s.config.Logger)
	err = engine.Start()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &fragment{
		service: dm.s,
		storage: engine,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func (dm *DMap) loadOrCreateFragment(part *partitions.Partition) (*fragment, error) {
	part.Lock()
	defer part.Unlock()

	// Critical section here. It should be protected by a lock.
	fg, ok := part.Map().Load(dm.fragmentName)
	if ok {
		// We already have the fragment.
		return fg.(*fragment), nil
	}

	f, err := dm.newFragment()
	if err != nil {
		return nil, err
	}

	part.Map().Store(dm.fragmentName, f)
	return f, nil
}

func (dm *DMap) loadFragment(part *partitions.Partition) (*fragment, error) {
	f, ok := part.Map().Load(dm.fragmentName)
	if !ok {
		return nil, errFragmentNotFound
	}
	return f.(*fragment), nil
}

var _ partitions.Fragment = (*fragment)(nil)

// lockFragment returns a live fragment with its lock held. The caller must unlock it.
func (dm *DMap) lockFragment(part *partitions.Partition, create, read bool) (*fragment, error) {
	for {
		if !dm.s.isAlive() {
			return nil, ErrServerGone
		}
		var f *fragment
		var err error
		if create {
			f, err = dm.loadOrCreateFragment(part)
		} else {
			f, err = dm.loadFragment(part)
		}
		if err != nil {
			return nil, err
		}
		if read {
			f.RLock()
		} else {
			f.Lock()
		}
		if f.ctx.Err() == nil {
			return f, nil
		}
		if read {
			f.RUnlock()
		} else {
			f.Unlock()
		}
		// An empty fragment can be retired between lookup and locking.
	}
}
