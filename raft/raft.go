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
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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
	id uint64

	Term uint64
	Vote uint64

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

	randomizedElectionTimeout int

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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress, len(c.peers)),
		State:            StateFollower,
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{}
	}
	lastIndex, err := c.Storage.LastIndex()
	if err != nil {
		log.Fatal(err)
	}
	r.Prs[r.id] = &Progress{Match: lastIndex, Next: lastIndex + 1}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		//判断心跳超时
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.reset()
			//广播心跳
			r.broadcastHeartbeat()
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			//竞选
			r.campaign()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	r.reset()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	//自增当前的任期号
	r.State = StateCandidate
	r.Term++
	//给自己投票
	r.votes = map[uint64]bool{r.id: true}
	r.Vote = r.id
	r.reset()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	msg := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.Term,
	}
	r.msgs = append(r.msgs, msg)
	r.reset()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m.From, m.Term)
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.campaign()
		}
	case pb.MessageType_MsgHeartbeat:
		if r.State == StateFollower {
			r.handleHeartbeat(&m)
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.broadcastHeartbeat()
		}
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResp(m.From, m.Term, m.Reject)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	default:
		return errors.New("unknown raft state")
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//if m.Term < r.Term {
	//	r.sendAppendResp(m.From, true)
	//	return
	//}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	//term, err := r.RaftLog.Term(m.Index)
	//if err != nil {
	//	log.Error(err)
	//	return
	//}
	//if term != m.Term {
	//	r.sendAppendResp(m.From, true)
	//	return
	//}
	//
	//var removeEntriesAfter = func(idx uint64) {}
	//var appendEntriesAfter = func(entries []*pb.Entry) {}
	//if len(m.Entries) > 0 {
	//	var i = 0
	//	for ; i < len(m.Entries); i++ {
	//		if m.Entries[i].Index > r.RaftLog.LastIndex() {
	//			break
	//		}
	//		term, err := r.RaftLog.Term(m.Entries[i].Index)
	//		if err != nil {
	//			log.Error(err)
	//			return
	//		}
	//		if term != m.Entries[i].Term {
	//			removeEntriesAfter(m.Entries[i].Index)
	//			break
	//		}
	//	}
	//	appendEntriesAfter(m.Entries[i:])
	//}
	//
	//if m.Commit > r.RaftLog.committed {
	//	lastIdx := m.Index
	//	if len(m.Entries) > 0 {
	//		lastIdx = m.Entries[len(m.Entries)-1].Index
	//	}
	//	r.RaftLog.committed = min(m.Commit, lastIdx)
	//}
	//r.sendAppendResp(m.From, true)
}

func (r *Raft) sendAppendResp(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m *pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResp(m.From, false)
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.sendHeartbeatResp(m.From, true)
}

func (r *Raft) sendHeartbeatResp(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) handleVoteResp(from, term uint64, reject bool) {
	if !reject && term == r.Term {
		r.votes[from] = true
		if len(r.votes)<<1 > len(r.Prs) {
			r.becomeLeader()
			return
		}
	}
	if term > r.Term {
		r.becomeFollower(term, 0)
	}
}

// 广播心跳
func (r *Raft) broadcastHeartbeat() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

// 广播投票请求
func (r *Raft) broadcastVote() {
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	lastIdx := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(lastIdx)
	if err != nil {
		log.Error(err)
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   lastIdx,
	}

	for peer := range r.Prs {
		if r.id == peer {
			continue
		}
		msg.To = peer
		r.msgs = append(r.msgs, msg)
	}
}

// 竞选
func (r *Raft) campaign() {
	r.becomeCandidate()
	//发送请求投票
	r.broadcastVote()
}

// 处理选票请求
func (r *Raft) handleVote(from, term uint64) {
	if r.Term > term {
		r.sendVoteResp(from, true)
		return
	}
	if r.Term == term && r.Vote != 0 && r.Vote != from {
		r.sendVoteResp(from, true)
		return
	}
	r.becomeFollower(term, from)
	r.sendVoteResp(from, false)
}

// 发送选票响应
func (r *Raft) sendVoteResp(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) reset() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}
