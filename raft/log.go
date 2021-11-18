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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	return &RaftLog{
		entries: entries,
		storage: storage,
		applied: firstIndex - 1,
		committed: firstIndex - 1,
		stabled: lastIndex,
		firstIndex: firstIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// storage.FirstIndex is the first entry which isn't in snapshot
	// entry in l.entries which index less than firstIndex should in snapshot
	// what we need to do is delete them
	firstIndex, _ := l.storage.FirstIndex()
	if len(l.entries) > 0 {
		first := l.entries[0].Index
		if first < firstIndex {
			l.entries = l.entries[firstIndex - firstIndex:]
		}
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		if (l.stabled-l.FirstIndex()+1 < 0) ||
			(l.stabled-l.FirstIndex()+1 > uint64(len(l.entries))) {
			return nil
		}
		return l.entries[l.stabled-l.FirstIndex()+1:]
	}
	return nil
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		i, _ := l.storage.FirstIndex()
		return i-1
	}
	return l.entries[0].Index
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		if l.committed-l.FirstIndex()+1 < 0 || l.applied-l.FirstIndex()+1 > l.LastIndex() {
			return nil
		}
		if l.applied-l.FirstIndex()+1 >= 0 && l.committed-l.FirstIndex()+1 <= uint64(len(l.entries)) {
			return l.entries[l.applied-l.FirstIndex()+1 : l.committed-l.FirstIndex()+1]
		}
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries) - 1].Index
	}else {
		index, err := l.storage.LastIndex()
		if err != nil {
			panic(err)
		}
		return index
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	for _, entry := range l.entries {
		if entry.Index == i {
			return entry.Term, nil
		}
	}
	return l.storage.Term(i)
}

func (l *RaftLog) LastLogTerm() uint64 {
	lastIndex := l.LastIndex()
	term, err := l.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	return term
}

func (l *RaftLog) DeleteFromIndex(index uint64) {
	for i, entry := range l.entries {
		if entry.Index == index {
			l.entries = l.entries[:i]
			break
		}
	}
	lastIndex := l.LastIndex()
	l.applied = min(l.applied, lastIndex)
	l.committed = min(l.committed, lastIndex)
	l.stabled = min(l.stabled, lastIndex)
}


