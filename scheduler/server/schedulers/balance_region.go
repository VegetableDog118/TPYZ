// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	curStores := cluster.GetStores()
	suitableStore := make([]*core.StoreInfo, 0)
	for _, store := range curStores {
		// a suitable store should be up and the down time cannot be longer than MaxStoreDownTime of the cluster
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStore = append(suitableStore, store)
		}
	}
	if len(suitableStore) == 0 {
		return nil
	}
	// then sort them according to their region size
	sort.Slice(suitableStore, func(i, j int) bool {
		return suitableStore[i].GetRegionSize() > suitableStore[j].GetRegionSize()
	})
	// find the region most suitable for moving in the store
	var targetRegion *core.RegionInfo
	curStore := suitableStore[0]
	for i:=0; i<len(suitableStore)-1; i++ {
		curStore = suitableStore[i]
		cluster.GetPendingRegionsWithLock(curStore.GetID(), func(container core.RegionsContainer) {
			targetRegion = container.RandomRegion(nil, nil)
		})
		// if get, break; otherwise find a follower region
		if targetRegion != nil {
			break
		}
		cluster.GetFollowersWithLock(curStore.GetID(), func(container core.RegionsContainer) {
			targetRegion = container.RandomRegion(nil, nil)
		})
		// if get, break; otherwise find a leader region
		if targetRegion != nil {
			break
		}
		cluster.GetLeadersWithLock(curStore.GetID(), func(container core.RegionsContainer) {
			targetRegion = container.RandomRegion(nil, nil)
		})
		if targetRegion != nil {
			break
		}
		// if still cannot pick out a region, try next store which has smaller region size
		// until all stores will have been tried
	}
	if targetRegion == nil {
		return nil}
	// select a store as the target to add
	// 10.27 to implement...
	tStores := cluster.GetRegionStores(targetRegion)
	targets := make([]*core.StoreInfo, 0)
	for _, store := range suitableStore {
		found := false
		for _, tStore := range tStores {
			if store.GetID() == tStore.GetID() {
				found = true
				break
			}
		}
		if !found {
			targets = append(targets, store)
		}
	}
	if len(targets)==0 {
		return nil
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})
	target := targets[0]
	// we have to make sure that the difference has to bigger than two times of the approximate size of the region
	difference := curStore.GetRegionSize() - target.GetRegionSize()
	if difference <= 2*targetRegion.GetApproximateSize() {
		return nil
	}
	// is the difference is big enough, the Scheduler should allocate a new peer on the target store
	// and create a move peer operator
	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		return nil
	}
	peerOperator, err := operator.CreateMovePeerOperator("balance_region", cluster, targetRegion, operator.OpBalance, curStore.GetID(), target.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return peerOperator
}
