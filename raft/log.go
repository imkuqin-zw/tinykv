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
	"github.com/pingcap-incubator/tinykv/log"
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
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, err := storage.InitialState()
	if err != nil {
		log.Fatalf("fault to new log, err: %s", err.Error())
		return nil
	}
	lo, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	entries := make([]pb.Entry, 0)
	if lo <= hi {
		entries, err = storage.Entries(lo, hi+1)
		if err != nil {
			panic(err)
		}
	}
	return &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   lo - 1,
		stabled:   hi,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	if len(l.entries) > 0 {
		sf, err := l.storage.FirstIndex()
		if err != nil {
			log.Fatalf("fault to get storage last index, err: %s", err.Error())
		}
		ef := l.entries[0].Index
		if sf > ef {
			entries := l.entries[sf-ef:]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.stabled+1 >= l.entries[0].Index {
		return l.entries[l.stabled-l.entries[0].Index+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		start := l.applied - l.entries[0].Index + 1
		end := min(l.committed-l.entries[0].Index+1, uint64(len(l.entries)))
		return l.entries[start:end]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var snapLast uint64
	if !IsEmptySnap(l.pendingSnapshot) {
		snapLast = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		return max(l.entries[len(l.entries)-1].Index, snapLast)
	}
	idx, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return max(idx-1, snapLast)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.entries[0].Index <= i {
		idx := i - l.entries[0].Index
		if idx >= uint64(len(l.entries)) {
			return 0, ErrUnavailable
		}
		return l.entries[idx].Term, nil
	}
	return l.storage.Term(i)
}

//清理index及其之后的日志
func (l *RaftLog) removeEntriesAfter(idx uint64) {
	l.stabled = min(l.stabled, idx-1)
	end := idx - l.entries[0].Index
	if end >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:end]
}

func (l *RaftLog) appendEntriesAfter(entries ...*pb.Entry) {
	for i := 0; i < len(entries); i++ {
		l.entries = append(l.entries, *entries[i])
	}
}

func (l *RaftLog) getEntries(start, end uint64) []pb.Entry {
	if l.entries[0].Index <= start && end-l.entries[0].Index <= uint64(len(l.entries)) {
		return l.entries[start-l.entries[0].Index : end-l.entries[0].Index]
	}

	entries, err := l.storage.Entries(start, end)
	if err != nil {
		log.Errorf("fault to get entries, err: %s", err.Error())
		return nil
	}
	return entries
}
