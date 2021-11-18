package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	Debug = false
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}



const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// 172

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	DPrintf("node [%v] start handle raft ready\n", d.Tag)
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	result, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		return
	}
	if result != nil {
		d.peerStorage.SetRegion(result.Region)
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regions[d.Region().Id] = d.Region()
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		d.ctx.storeMeta.Unlock()
	}
	// send messages
	if len(rd.Messages) != 0 {
		d.Send(d.ctx.trans, rd.Messages)
	}

	// problem: no entry to apply!
	if len(rd.CommittedEntries) == 0 {
		DPrintf("node [%v] has nothing to apply!\n", d.Tag)
		d.RaftGroup.Advance(rd)
		return
	}

	// apply committed entries
	if len(rd.CommittedEntries) != 0 {
		for _, entry := range rd.CommittedEntries {
			kvWB := new(engine_util.WriteBatch)
			if entry.EntryType == eraftpb.EntryType_EntryConfChange {
				DPrintf("node [%v] start applyConfChange\n", d.Tag)
				d.applyConfChange(&entry, kvWB)
			}else {
				DPrintf("node [%v] start applyNormalEntries\n", d.Tag)
				d.applyNormalEntries(&entry, kvWB)
			}
			// update the applyState and write to kvDB
			d.peerStorage.applyState.AppliedIndex = entry.Index
			setErr := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if setErr != nil {
				panic(setErr)
			}
			if d.stopped {
				return
			}
			writeErr := kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			if writeErr != nil {
				panic(writeErr)
			}
		}
	}
	d.RaftGroup.Advance(rd)
	DPrintf("node [%v] finished handle raft ready\n", d.Tag)
}

func (d *peerMsgHandler) applyNormalEntries(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	// unmarshal entry and get the msg
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(entry.Data)
	if err != nil {
		panic(err)
	}

	// debug info
	if Debug {
		hasRequest := len(msg.Requests) != 0
		if hasRequest {
			DPrintf("node [%v] has normal request\n", d.Tag)
		}
		hasAdminReq := msg.AdminRequest != nil
		if hasAdminReq {
			DPrintf("node [%v] has admin request\n", d.Tag)
		}
	}


	if len(msg.Requests) != 0 {
		// apply the entry
		req := msg.Requests[0]
		DPrintf("node [%v] apply normal entry, type= [%v]\n", d.Tag, req.CmdType)
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
		case raft_cmdpb.CmdType_Get:
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
		case raft_cmdpb.CmdType_Snap:
		}
		// propose the callback
		if len(d.proposals) != 0 {
			propose := d.proposals[0]
			for propose.index < entry.Index {
				propose.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				d.proposals = d.proposals[1:]
				if len(d.proposals) == 0 {
					return
				}
				propose = d.proposals[0]
			}
			if propose.index == entry.Index {
				if propose.term != entry.Term {
					DPrintf("it's a stale request!\n")
					NotifyStaleReq(entry.Term, propose.cb)
					d.proposals = d.proposals[1:]
					return
				}
				resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
				switch req.CmdType {
				case raft_cmdpb.CmdType_Get:
					val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
					resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: val}}}
				case raft_cmdpb.CmdType_Put:
					resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}}}
				case raft_cmdpb.CmdType_Delete:
					resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}}}
				case raft_cmdpb.CmdType_Snap:
					if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
						propose.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
						return
					}
					resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
					propose.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				}
				propose.cb.Done(resp)
				d.proposals = d.proposals[1:]
			}
		}
	}else if msg.AdminRequest != nil{
		req := msg.AdminRequest
		if req.CmdType == raft_cmdpb.AdminCmdType_CompactLog {
			compactLog  := req.CompactLog
			compactIdx  := compactLog.CompactIndex
			compactTerm := compactLog.CompactTerm
			applyState  := d.peerStorage.applyState
			// namely update the RaftTruncatedState which is in the RaftApplyState
			if applyState.TruncatedState.Index <= compactIdx {
				applyState.TruncatedState.Index = compactIdx
				applyState.TruncatedState.Term = compactTerm
				kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
				// schedule a task to raftLog-gc worker by ScheduleCompacting
				d.ScheduleCompactLog(applyState.TruncatedState.Index)
			}
		}
		if req.CmdType == raft_cmdpb.AdminCmdType_Split {
			DPrintf3B("start apply split!\n")
			// code in 3BC
			// call chain: generate key -> handle MsgTypeSplitRegion -> onPrepareSplitRegion ->
			// onAskSplit -> sendAdminRequest(Split) -> router.sendRaftCommand -> handle raftCmd
			/*if len(d.proposals) > 0 {
				propose := d.proposals[0]
				for propose.index < entry.Index {
					propose.cb.Done(ErrResp(&util.ErrStaleCommand{}))
					d.proposals = d.proposals[1:]
					if len(d.proposals) == 0 {
						return
					}
					propose = d.proposals[0]
				}
				if propose.index == entry.Index {
					if propose.term != entry.Term {
						NotifyStaleReq(entry.Term, propose.cb)
						d.proposals = d.proposals[1:]
						return
					}
					d.handleApplySplit(msg, kvWB, propose)
					d.proposals = d.proposals[1:]
				}
			}*/

			if msg.Header.RegionId != d.regionId {
				regionNotFound := &util.ErrRegionNotFound{RegionId: msg.Header.RegionId}
				resp := ErrResp(regionNotFound)
				d.handleProposals(resp, entry)
				return
			}
			err = util.CheckRegionEpoch(msg, d.Region(), true)
			if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
				siblingRegion := d.findSiblingRegion()
				if siblingRegion != nil {
					errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
				}
				d.handleProposals(ErrResp(errEpochNotMatching), entry)
				return
			}

			err = util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
			if err != nil {
				d.handleProposals(ErrResp(err), entry)
				return
			}

			peers := make([]*metapb.Peer, 0)
			length := len(d.Region().Peers)
			// sort to ensure the order between different peers
			for i := 0; i < length; i++ {
				for j := 0; j < length-i-1; j++ {
					if d.Region().Peers[j].Id > d.Region().Peers[j+1].Id {
						temp := d.Region().Peers[j+1]
						d.Region().Peers[j+1] = d.Region().Peers[j]
						d.Region().Peers[j] = temp
					}
				}
			}
			for i, peer := range d.Region().Peers {
				// println(i, peer.Id, peer.StoreId)
				peers = append(peers, &metapb.Peer{Id: req.Split.NewPeerIds[i], StoreId: peer.StoreId})
			}
			region := &metapb.Region{
				Id:       req.Split.NewRegionId,
				StartKey: req.Split.SplitKey,
				EndKey:   d.Region().EndKey,
				RegionEpoch: &metapb.RegionEpoch{
					ConfVer: 1,
					Version: 1,
				},
				Peers: peers,
			}
			newpeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, region)
			if err != nil {
				panic(err)
			}
			d.ctx.storeMeta.Lock()
			d.Region().EndKey = req.Split.SplitKey
			// update RegionEpoch
			d.Region().RegionEpoch.Version++
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
			d.ctx.storeMeta.regions[req.Split.NewRegionId] = region
			d.ctx.storeMeta.Unlock()
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)

			// start
			d.ctx.router.register(newpeer)
			_ = d.ctx.router.send(req.Split.NewRegionId,
				message.Msg{RegionID: req.Split.NewRegionId, Type: message.MsgTypeStart})
			// callback
			resp := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_Split,
					Split: &raft_cmdpb.SplitResponse{
						Regions: []*metapb.Region{region, d.Region()},
					},
				},
			}
			d.handleProposals(resp, entry)
			log.Infof("finish Split")
		}
	}

}

func (d *peerMsgHandler) handleProposals(resp *raft_cmdpb.RaftCmdResponse, entry *eraftpb.Entry) {
	if len(d.proposals) > 0 {
		p := d.proposals[0]
		// println("Callback No.", p.index, "proposal", "entry.Index:", entry.Index)
		for p.index < entry.Index {
			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			d.proposals = d.proposals[1:]
			if len(d.proposals) == 0 {
				return
			}
			p = d.proposals[0]
		}
		if p.index == entry.Index {
			if p.term != entry.Term {
				NotifyStaleReq(entry.Term, p.cb)
			} else {
				p.cb.Done(resp)
			}
			d.proposals = d.proposals[1:]
		}
	}
}

func (d *peerMsgHandler) handleApplySplit(msg *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch, propose *proposal) {
	// There are more errors need to be considered: ErrRegionNotFound, ErrKeyNotInRegion, ErrEpochNotMatch
	var err error
	if msg.Header.RegionId != d.regionId {
		propose.cb.Done(ErrResp(&util.ErrRegionNotFound{RegionId: msg.Header.RegionId}))
		return
	}
	// the logic of handling regionEpoch refer to preProposeRaftCommand()
	err = util.CheckRegionEpoch(msg, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		propose.cb.Done(ErrResp(errEpochNotMatching))
		return
	}
	if err = util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region()); err != nil {
		propose.cb.Done(ErrResp(err))
		return
	}
	// no err to handle, begin split operation
	split := msg.AdminRequest.Split

	//d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: d.Region()})
	//DPrintf3B("delete region [%v] in regionRange\n", d.Region().Id)
	length := len(d.Region().Peers)
	for i := 0; i < length; i++ {
		for j := 0; j < length-i-1; j++ {
			if d.Region().Peers[j].Id > d.Region().Peers[j+1].Id {
				temp := d.Region().Peers[j+1]
				d.Region().Peers[j+1] = d.Region().Peers[j]
				d.Region().Peers[j] = temp
			}
		}
	}
	peers := make([]*metapb.Peer, 0)
	for i, peer := range d.Region().Peers {
		peers = append(peers, &metapb.Peer{Id: split.NewPeerIds[i], StoreId: peer.StoreId})
	}
	region := &metapb.Region{
		Id: split.NewRegionId,
		StartKey: split.SplitKey,
		EndKey: d.Region().EndKey,
		RegionEpoch: &metapb.RegionEpoch{Version: InitEpochVer, ConfVer: InitEpochConfVer},
		Peers: peers,
	}
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, region)
	if err != nil {
		panic(err)
	}
	d.ctx.storeMeta.Lock()
	d.Region().RegionEpoch.Version++
	d.Region().EndKey = split.SplitKey
	DPrintf3B("replace or insert region [%v] in regionRange\n", d.Region().Id)
	DPrintf3B("replace or insert region [%v] in regionRange\n", region.Id)
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	d.ctx.storeMeta.regions[split.NewRegionId] = region
	d.ctx.storeMeta.Unlock()
	meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)

	d.ctx.router.register(newPeer)
	d.ctx.router.send(region.Id, message.NewPeerMsg(message.MsgTypeStart, region.Id, nil))

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{d.Region(), region}},
		},
	}
	propose.cb.Done(resp)
}

func (d *peerMsgHandler) applyConfChange(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	cc := &eraftpb.ConfChange{}
	err := cc.Unmarshal(entry.Data)
	if err != nil {
		panic(err)
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err = msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	// the problem is where to call the ApplyConfChange in RawNode
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		isPeerInReagion := false
		for _, peer := range d.Region().Peers {
			if peer.Id == cc.NodeId {
				isPeerInReagion = true
				break
			}
		}
		if !isPeerInReagion {
			d.ctx.storeMeta.Lock()
			d.Region().RegionEpoch.ConfVer++
			newPeer := msg.AdminRequest.ChangePeer.Peer
			if d.IsLeader() {
				d.PeersStartPendingTime[newPeer.Id] = time.Now()
			}
			d.Region().Peers = append(d.Region().Peers, newPeer)
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			// Update the regionState in StoreMeta of GlobalCtx
			d.ctx.storeMeta.regions[d.regionId] = d.Region()
			d.ctx.storeMeta.Unlock()
			d.insertPeerCache(newPeer)

			DPrintf3B("[%v] add new peer, id = [%v]\n", d.Tag, newPeer.Id)
		}

	case eraftpb.ConfChangeType_RemoveNode:
		// if peer itself is the delete target, call destroyPeer() to stop the raft module
		if cc.NodeId == d.PeerId() {
			// kvWB.DeleteMeta(meta.ApplyStateKey(d.regionId))
			d.destroyPeer()
			return
		}
		for i, peer := range d.Region().Peers {
			if peer.Id == cc.NodeId {
				// found the target peer
				d.ctx.storeMeta.Lock()
				d.Region().RegionEpoch.ConfVer++
				if d.IsLeader() {
					delete(d.PeersStartPendingTime, peer.Id)
				}
				d.Region().Peers = append(d.Region().Peers[:i], d.Region().Peers[i+1:]...)
				meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
				d.removePeerCache(peer.Id)
				// Update the regionState in StoreMeta of GlobalCtx
				d.ctx.storeMeta.regions[d.regionId] = d.Region()
				d.ctx.storeMeta.Unlock()
				d.removePeerCache(cc.NodeId)
				break
			}
		}
	}
	d.peer.RaftGroup.ApplyConfChange(*cc)
	DPrintf3B("[%v] now peers=[%v]\n", d.Tag, d.RaftGroup.Raft.Prs)
	// call back to the original proposal
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer,ChangePeer: &raft_cmdpb.ChangePeerResponse{}},
	}

	if len(d.proposals) > 0 {
		propose := d.proposals[0]
		for propose.index < entry.Index {
			propose.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			d.proposals = d.proposals[1:]
			if len(d.proposals) == 0 {
				return
			}
			propose = d.proposals[0]
		}
		if propose.index == entry.Index {
			if propose.term != entry.Term {
				NotifyStaleReq(entry.Term, propose.cb)
				d.proposals = d.proposals[1:]
				return
			}else {
				propose.cb.Done(resp)
				d.proposals = d.proposals[1:]
			}
		}
	}

	/*if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}*/
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	// fmt.Printf("msg type is [%v]", msg.Type)
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		DPrintf3B("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// only propose one request
	// propose adminRequest if adminRequest exists
	// otherwise propose the first request in slice request
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	}else if len(msg.Requests) != 0 {
		for len(msg.Requests) > 0 {
			d.proposeCommonRequest(msg, cb)
		}
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// whether is need to append new proposal depends on how the message works
	// if the AdminCmd is proposed on a target peer, such as transferLeader and compactLog
	// which targets at the leader and don't need to handle with entries, just done() the callback
	// if the Cmd is proposed on other peers in RaftGroup by appending entry,
	// and wait entry committed before applying. We need spread the callback to peers through []proposals
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		// fmt.Println("handling compactLog...")
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		if err = d.RaftGroup.Propose(data); err != nil {
			cb.Done(ErrResp(err))
		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		transfer := msg.AdminRequest.TransferLeader
		d.RaftGroup.TransferLeader(transfer.Peer.Id)
		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		}
		cb.Done(resp)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		DPrintf("[%v] propose change peer, peerId=[%v]\n", d.Tag, msg.AdminRequest.ChangePeer.Peer.Id)
		context, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		cc := eraftpb.ConfChange{Context: context, ChangeType: msg.AdminRequest.ChangePeer.ChangeType, NodeId: msg.AdminRequest.ChangePeer.Peer.Id}
		propose := &proposal{cb: cb, index: d.nextProposalIndex(), term: d.Term()}
		d.proposals = append(d.proposals, propose)
		err = d.RaftGroup.ProposeConfChange(cc)
		if err != nil {
			propose.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			return
		}
	case raft_cmdpb.AdminCmdType_Split:
		err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		data, _ := msg.Marshal()
		propose := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, propose)
		d.RaftGroup.Propose(data)
	}
	
}

func (d *peerMsgHandler) proposeCommonRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	cr := msg.Requests[0]
	DPrintf("node [%v] propose cmd, type is [%v]\n", d.Tag, cr.CmdType)
	var key []byte
	switch cr.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = cr.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = cr.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = cr.Delete.Key
	case raft_cmdpb.CmdType_Snap:
	}
	checkErr := util.CheckKeyInRegion(key, d.Region())
	if checkErr != nil && cr.CmdType != raft_cmdpb.CmdType_Snap{ // keyNotFound
		DPrintf("key not found, type is [%v]\n", cr.CmdType)
		cb.Done(ErrResp(checkErr))
		msg.Requests = msg.Requests[1:]
		return
	}
	d.proposals = append(d.proposals, &proposal{
		term: d.Term(),
		index: d.nextProposalIndex(),
		cb: cb,
	})
	data, marErr := msg.Marshal()
	if marErr != nil {
		panic(marErr)
	}
	d.RaftGroup.Propose(data)
	msg.Requests = msg.Requests[1:]
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		DPrintf3B("isInitialized=[%v]\n", isInitialized)
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
