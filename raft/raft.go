// Copyright 2015 The etcd Authors
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

package raft

import (
	"errors"
	"fmt"
	"github.com/gogo/protobuf/sortkeys"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

const Debug = false
const Debug3B = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

func DPrintf3B(format string, a ...interface{}) (n int, err error) {
	if Debug3B {
		fmt.Printf(format, a...)
	}
	return
}

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64       //index

	Term uint64
	Vote uint64		//voteFor

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// actual election timeout
	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	prs := make(map[uint64]*Progress, len(c.peers))
	for _, peer := range c.peers {
		prs[peer] = &Progress{}
	}
	//DPrintf("start a new Node: id=[%v]\n", c.ID)
	raft := &Raft{
		id: 						c.ID,
		Vote: 						hardState.Vote,
		Lead: 						None,
		State: 						StateFollower,
		Term: 						hardState.Term,
		electionElapsed: 			0,
		heartbeatElapsed: 			0,
		msgs: 						make([]pb.Message, 0),
		RaftLog: 					newLog(c.Storage),
		electionTimeout: 			c.ElectionTick,
		randomElectionTimeout: 		c.ElectionTick + rand.Intn(c.ElectionTick),
		Prs: 						prs,
		votes: 						make(map[uint64]bool, 0),
	}
	raft.RaftLog.committed = hardState.Commit
	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if to != r.id && r.State == StateLeader {
		//matchIndex := r.Prs[to].Match
		entries := make([]*pb.Entry, 0)
		preLogIndex := r.Prs[to].Next - 1
		preLogTerm, err:= r.RaftLog.Term(preLogIndex)
		firstIndex := r.RaftLog.FirstIndex()
		DPrintf3B("[%v]firstIndex=[%v], preLogIndex=[%v]\n", to, firstIndex, preLogIndex)
		if err != nil || preLogIndex < firstIndex - 1{
			r.sendSnapshot(to)
			return true
		}
		DPrintf("before [%v] append to [%v], preLogIndex=[%v], lastIndex=[%v], raftLog=[%v] \n",
			r.id, to, preLogIndex, r.RaftLog.LastIndex(), r.RaftLog.entries)
		for _, entry := range r.RaftLog.entries {
			if entry.Index >= r.Prs[to].Next {
				DPrintf("append one!, entry=[%v], term[%v], index[%v]\n", entry, entry.Term, entry.Index)
				entries = append(entries, &pb.Entry{
					Term: entry.Term,
					Index: entry.Index,
					Data: entry.Data,
					EntryType: entry.EntryType,
				})
			}
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From: r.id,
			To: to,
			Term: r.Term,
			Commit: r.RaftLog.committed,
			Index: preLogIndex,
			LogTerm: preLogTerm,
			Entries: entries,
		}
		DPrintf("[%v] send append to node [%v], Index=[%v], logTerm=[%v], preLogIndex=[%v], preLogTerm=[%v], entries=[%v]\n",
			r.id, to, r.RaftLog.LastIndex(), r.RaftLog.LastLogTerm(), preLogIndex, preLogTerm, entries)
		r.msgs = append(r.msgs, msg)

		return true
	}
	return false
}

func (r *Raft) sendSnapshot(to uint64) {
	if _, ok := r.Prs[to]; !ok {
		return
	}
	var snapshot pb.Snapshot
	var err error = nil
	if IsEmptySnap(r.RaftLog.pendingSnapshot) {
		// fmt.Printf("reach storage!\n")
		snapshot, err = r.RaftLog.storage.Snapshot()
	} else {
		// fmt.Printf("reach pending!\n")
		snapshot = *r.RaftLog.pendingSnapshot
	}
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	DPrintf3B("[%v] send Snapshot to [%v]\n", r.id, to)
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	return
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if to != r.id && r.State == StateLeader {
		r.heartbeatElapsed = 0
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, From: r.id, To: to, Term: r.Term, Commit: util.RaftInvalidIndex})
	}

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// use logic clock instead of physics clock , by using timeout and elapse
	r.electionElapsed ++
	r.heartbeatElapsed ++

	if r.randomElectionTimeout < r.electionElapsed && (r.State == StateFollower || r.State == StateCandidate) {
		// initiate a election
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, Term: r.Term})
	}

	if r.heartbeatTimeout < r.heartbeatElapsed && r.State == StateLeader {
		// do the heartbeat
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, Term: r.Term})
	}
}

// thinking of func becomeXXX often reset some raft attribute, we definite a reset func.
func (r *Raft) reset() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	// reset the randomElectionTimeout
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Vote = None
	r.Lead = None
	r.votes = make(map[uint64]bool, 0)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset()
	r.State = StateFollower
	if term > r.Term {
		r.Term = term
		r.Lead = lead
	}
	DPrintf("node [%v] become [%v], should be [%v], term=[%v]\n", r.id, r.State, StateFollower, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset()
	r.State = StateCandidate
	// update the term
	r.Term++
	// vote for itself
	r.Vote = r.id
	r.votes[r.id] = true
	DPrintf("node [%v] become [%v], should be [%v], term=[%v]\n", r.id, r.State, StateCandidate, r.Term)

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateLeader {
		r.reset()
		r.State = StateLeader
		r.Lead = r.id
		DPrintf("node [%v] become [%v], should be [%v], lastIndex=[%v]\n", r.id, r.State, StateLeader, r.RaftLog.LastIndex())

		// add a no-op entry
		preLastIndex := r.RaftLog.LastIndex()
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Index: preLastIndex + 1,
			Term: r.Term,
		})

		// initialize the nextIndex and matchIndex
		for peer := range r.Prs {
			if peer == r.id {
				r.Prs[peer] = &Progress{
					Match: r.RaftLog.LastIndex(),
					Next: r.RaftLog.LastIndex() + 1,
				}
			}else {
				r.Prs[peer] = &Progress{
					Match: 0,
					Next: preLastIndex + 1,
				}
			}
		}
		DPrintf("now index=[%v], logTerm=[%v]\n", r.RaftLog.LastIndex(), r.RaftLog.LastLogTerm())
		// send the no-op entry to other nodes
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgPropose,
		})
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// first do a pre-logic : check the term
	DPrintf3B("[%v] receive message from [%v], type=[%v]\n", r.id, m.From, m.MsgType)
	if _, ok := r.Prs[r.id]; !ok {
		// when a MessageType_MsgTimeoutNow arrives at a node that has been removed from the group, nothing happens.
		return nil
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			// if node has no peer, it can become Leader without election
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}else{
				for peer := range r.Prs {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgRequestVote:
			// do the vote logic
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow()
		case pb.MessageType_MsgTransferLeader:
			// transit the message to the leader
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			// if node has no peer, it can become Leader without election
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}else{
				for peer := range r.Prs {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgRequestVote:
			// because we check the term at the beginning of Step(), so the term from message must less than r.term
			// just reject it
			r.handleRequestVote(m)
			//r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Reject: true, Term: r.Term})
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			// count and if majority of peers voted, node become leader
			approve, reject := 0, 0
			for _, vote := range r.votes {
				if vote {
					approve++
				}else {
					reject++
				}
			}
			if approve > len(r.Prs)/2 {
				r.becomeLeader()
			}else if reject > len(r.Prs)/2 {
				r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgHeartbeat:
			// get the heartbeat from leader, stop the election
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow()
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Reject: true, Term: r.Term})
		case pb.MessageType_MsgBeat:
			// only Leader can dispose this message
			for peer := range r.Prs {
				if peer == r.id {
					continue
				}
				r.sendHeartbeat(peer)
			}
		case pb.MessageType_MsgRequestVoteResponse:
			// had become leader, ignore the message
		case pb.MessageType_MsgHeartbeatResponse:
			if m.Commit < r.RaftLog.committed{
				DPrintf("node[%v] send append to[%v] because of heartbeat\n", r.id, m.From)
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgPropose:
			r.handleMsgPropose(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		}
	}
	return nil
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	transferee := r.leadTransferee
	// check the transferee's qualification
	if r.Prs[transferee].Match != r.RaftLog.LastIndex() {
		// help the transferee
		r.sendAppend(transferee)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From: r.id,
		To: transferee,
		Term: r.Term,
	})
}

func (r *Raft) handleTimeoutNow() {
	r.electionElapsed = 0
	r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id})
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	if r.State == StateLeader {
		lastIndex := r.RaftLog.LastIndex()
		// add the entry to its log
		if m.Entries != nil {
			for _, entry := range m.Entries {
				entry.Term = r.Term
				entry.Index = lastIndex + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
		}
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.RaftLog.LastIndex()
			return
		}
		r.Prs[r.id] = &Progress{
			Match: r.RaftLog.LastIndex(),
			Next: r.RaftLog.LastIndex() + 1,
		}
		for peer := range r.Prs {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) sendRequestVote(peerId uint64) {
	if peerId != r.id {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From: r.id,
			To: peerId,
			Term: r.Term,
			Index: r.RaftLog.LastIndex(),
			LogTerm: r.RaftLog.LastLogTerm(),
		})
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true

	DPrintf("Node [%v] handle vote to [%v], state[%v], term[%v], Index[%v], logTerm[%v]\n",
		r.id, m.From, r.State, r.Term, r.RaftLog.LastIndex(), r.RaftLog.LastLogTerm())
	DPrintf("candidate node: term[%v], Index[%v], logTerm[%v]\n", m.Term, m.Index, m.LogTerm)
	if m.Term < r.Term{
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Reject: reject, Term: r.Term})
		DPrintf("Reject: [%v]\n\n", reject)
		return
	}
	// vote logic(after term check):
	// if node didn't vote for anyone or had voted for the candidate in message, go on the logic, else reject
	// if node's last log term less than candidate's, approve, if more, reject, else go on the logic
	// compare the last log index, if node's more than candidate's, reject, less or equal, approve
	if (r.Vote == None || r.Vote == m.From) && (r.RaftLog.LastLogTerm() < m.LogTerm || (r.RaftLog.LastLogTerm() == m.LogTerm &&
		r.RaftLog.LastIndex() <= m.Index)) {
		reject = false
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term, Reject: reject})
	DPrintf("Reject: [%v]\n\n", reject)
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: to,
		Index: index,
		Reject: reject,
		Term: r.Term,
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	DPrintf("node [%v] handle append, state=[%v], term=[%v], lastIndex=[%v], lastTerm=[%v], preIndex=[%v]\n",
		r.id, r.State, r.Term, r.RaftLog.LastIndex(), r.RaftLog.LastLogTerm(), m.Index)
	r.electionElapsed = 0
	if r.Term <= m.Term && r.State != StateFollower {
		r.becomeFollower(m.Term, m.From)
	}
	lastIndex := r.RaftLog.LastIndex()

	if m.Term != None && r.Term > m.Term {
		// stale and reject
		DPrintf("node[%v] reject append, reason: term stale\n", r.id)
		r.sendAppendResponse(m.From, true, 0)
		return
	}
	r.Lead = m.From
	if m.Index > lastIndex {
		// not match certainly
		DPrintf("node[%v] reject append, reason: do not have such index\n", r.id)
		r.sendAppendResponse(m.From,true, r.RaftLog.LastIndex())
		return
	}
	// index match

	logTerm, _ := r.RaftLog.Term(m.Index)


	if logTerm != m.LogTerm {
		DPrintf("node[%v] reject append, reason: term not match\n", r.id)
		r.sendAppendResponse(m.From,true, r.RaftLog.LastIndex())
		return
	}


	for i, entry := range m.Entries {
		if entry.Index <= r.RaftLog.LastIndex() {
			// in this case, maybe we need to delete some entry
			logTerm, _ := r.RaftLog.Term(entry.Index)
			if logTerm != entry.Term {
				r.RaftLog.DeleteFromIndex(entry.Index)
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index - 1)
			}
		}else {
			for _, entry := range m.Entries[i:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
			break
		}
	}

	DPrintf("append success, now node[%v] raftLog=[%v]\n", r.id, r.RaftLog.entries)

	// update the commit
	if m.Commit > r.RaftLog.committed {
		committed := min(m.Commit, m.GetIndex()+uint64(len(m.GetEntries())))
		r.RaftLog.committed = min(committed, r.RaftLog.LastIndex())
		DPrintf("node[%v] commit update to[%v]\n", r.id, r.RaftLog.committed)
	}

	DPrintf("node[%v] send appendResponse to[%v], reject=[%v], lastIndex=[%v]\n", r.id, m.From, false, r.RaftLog.LastIndex())
	r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())

}

func (r *Raft) handleAppendResponse(m pb.Message) {
	DPrintf("node[%v] receive appendResponse from[%v], reject=[%v]\n", r.id, m.From, m.Reject)
	if m.Reject {
		r.Prs[m.From].Next --
		DPrintf("Leader[%v] resend append to[%v], next=[%v], match=[%v]\n", r.id, m.From, r.Prs[m.From].Next, r.Prs[m.From].Match)
		r.sendAppend(m.From)
		return
	}
	// update next and match
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index

	}
	DPrintf("matchIndex update, node [%v] matchIndex[%v]\n", m.From, r.Prs[m.From].Match)
	r.findCommitIndex()

}

func (r *Raft) findCommitIndex() {
	// check state
	if r.State != StateLeader {
		return
	}
	commitChanged := false
	// update commit
	sortedMatchIndex := make([]uint64, 0)

	for _, progress := range r.Prs {
		sortedMatchIndex = append(sortedMatchIndex, progress.Match)
	}
	DPrintf("matchIndex before sorting = [%v]\n", sortedMatchIndex)
	sortkeys.Uint64s(sortedMatchIndex)
	for i, j := 0, len(sortedMatchIndex)-1; i<j; i, j = i+1, j-1 {
		sortedMatchIndex[i], sortedMatchIndex[j] = sortedMatchIndex[j], sortedMatchIndex[i]
	}
	DPrintf("sortedMatchIndex=[%v]\n", sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(sortedMatchIndex)/2]
	DPrintf("newCommitIndex=[%v]\n", newCommitIndex)
	curTerm, _ := r.RaftLog.Term(newCommitIndex)
	DPrintf("curTerm=[%v], r.Term=[%v], r.RaftLog=[%v]\n", curTerm, r.Term, r.RaftLog.entries)
	if newCommitIndex > r.RaftLog.committed && curTerm == r.Term {
		r.RaftLog.committed = newCommitIndex
		commitChanged = true
		DPrintf("Leader update committed to [%v]\n", r.RaftLog.committed)
	}

	// if commit changed, broadcast to other node
	if commitChanged {
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Lead = m.From
	//r.RaftLog.committed = m.Commit
	if m.Term < r.Term {
		r.Lead = None
	}
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, From: r.id, To: m.From, Term: r.Term, Commit: r.RaftLog.committed})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	DPrintf3B("[%v] handle snapShot from [%v]\n", r.id, m.From)
	r.electionElapsed = 0
	if r.Term > m.Term || m.Snapshot.Metadata.Term < r.RaftLog.committed {
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.Lead = m.From
	index := m.Snapshot.Metadata.Index
	term := m.Snapshot.Metadata.Term
	r.RaftLog.stabled = index
	r.RaftLog.committed = index
	r.RaftLog.applied = index
	r.RaftLog.firstIndex = index + 1
	// amend the raftLog entries
	if len(r.RaftLog.entries) > 0 {
		if index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = []pb.Entry{}
		}else if index > r.RaftLog.FirstIndex() {
			r.RaftLog.entries = r.RaftLog.entries[index-r.RaftLog.firstIndex:]
		}
	}
	if len(r.RaftLog.entries) == 0 {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term: term,
			Index: index,
		})
	}
	// ConfState contains the current membership information of the raft group
	confState := m.Snapshot.Metadata.ConfState
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range confState.Nodes {
		r.Prs[peer] = &Progress{
			Match: 0,
			Next: m.Snapshot.Metadata.Index + 1,
		}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	DPrintf3B("[%v] finished handle snapshot\n", r.id)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	//r.PendingConfIndex = 0
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Match: 0, Next: 1}
	}
	r.PendingConfIndex = 0
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	//r.PendingConfIndex = 0
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
	}
	r.findCommitIndex()
	r.PendingConfIndex = 0
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term: r.Term,
		Commit: r.RaftLog.committed,
		Vote: r.Vote,
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		RaftState: r.State,
		Lead: r.Lead,
	}
}
