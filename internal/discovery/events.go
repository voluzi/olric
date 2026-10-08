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

import "github.com/hashicorp/memberlist"

// Memberlist invokes event delegates under its node lock. Capture membership
// here; Members() exposes Node pointers whose metadata can change after return.
type memberEvents struct {
	discovery *Discovery
	ch        chan memberlist.NodeEvent
}

func (e *memberEvents) notify(event memberlist.NodeEventType, node *memberlist.Node) {
	member, _ := NewMemberFromMetadata(node.Meta)
	e.discovery.membersMtx.Lock()
	if event == memberlist.NodeLeave {
		delete(e.discovery.members, node.Name)
	} else {
		e.discovery.members[node.Name] = member
	}
	e.discovery.membersMtx.Unlock()

	snapshot := *node
	e.ch <- memberlist.NodeEvent{Event: event, Node: &snapshot}
}

func (e *memberEvents) NotifyJoin(node *memberlist.Node) {
	e.notify(memberlist.NodeJoin, node)
}

func (e *memberEvents) NotifyLeave(node *memberlist.Node) {
	e.notify(memberlist.NodeLeave, node)
}

func (e *memberEvents) NotifyUpdate(node *memberlist.Node) {
	e.notify(memberlist.NodeUpdate, node)
}

func ToClusterEvent(e memberlist.NodeEvent) *ClusterEvent {
	return &ClusterEvent{
		Event:    e.Event,
		NodeName: e.Node.Name,
		NodeAddr: e.Node.Addr,
		NodePort: e.Node.Port,
		NodeMeta: e.Node.Meta,
	}
}

func (d *Discovery) handleEvent(event memberlist.NodeEvent) {
	d.clusterEventsMtx.RLock()
	defer d.clusterEventsMtx.RUnlock()

	for _, ch := range d.eventSubscribers {
		if event.Node.Name == d.member.Name {
			continue
		}
		ch <- ToClusterEvent(event)
	}
}

// eventLoop awaits for messages from memberlist and broadcasts them to  event listeners.
func (d *Discovery) eventLoop(eventsCh chan memberlist.NodeEvent) {
	defer d.wg.Done()

	for {
		select {
		case e := <-eventsCh:
			d.handleEvent(e)
		case <-d.ctx.Done():
			return
		}
	}
}

func (d *Discovery) SubscribeNodeEvents() chan *ClusterEvent {
	d.clusterEventsMtx.Lock()
	defer d.clusterEventsMtx.Unlock()

	ch := make(chan *ClusterEvent, eventChanCapacity)
	d.eventSubscribers = append(d.eventSubscribers, ch)
	return ch
}
