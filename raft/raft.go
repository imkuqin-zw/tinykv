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
	"os"

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

	voteAgreeCount  uint32
	voteRejectCount uint32
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		log.Fatalf("fault to new log, err: %s", err.Error())
		return nil
	}
	r := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress, len(c.peers)),
		State:            StateFollower,
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Vote:             hardState.Vote,
		Term:             hardState.Term,
	}
	r.RaftLog.applied = c.Applied
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
	lastIndex := r.RaftLog.LastIndex()
	process := r.Prs[to]
	preLogIndex := process.Next - 1
	if lastIndex < preLogIndex {
		return true
	}
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		if err == ErrCompacted {
			// 发送快照
			r.sendSnap(to)
			return false
		}
		log.Errorf("fault to send append to %d, err: %s", to, err.Error())
		return false
	}

	entries := r.RaftLog.getEntries(preLogIndex+1, lastIndex+1)
	sendEntries := make([]*pb.Entry, 0, len(entries))
	for _, en := range entries {
		sendEntries = append(sendEntries, &pb.Entry{
			EntryType: en.EntryType,
			Term:      en.Term,
			Index:     en.Index,
			Data:      en.Data,
		})
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: sendEntries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
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
			r.resetTick()
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
	r.votes = nil
	r.voteAgreeCount = 0
	r.voteRejectCount = 0
	r.resetTick()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	//自增当前的任期号
	r.State = StateCandidate
	r.Term++
	//给自己投票
	r.votes = map[uint64]bool{r.id: true}
	r.voteAgreeCount = 1
	r.voteRejectCount = 0
	r.Vote = r.id
	r.Lead = None
	r.resetTick()
	if r.voteAgreeCount > uint32(len(r.Prs))>>1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	log.Debugf("%d become leader", r.id)
	r.State = StateLeader
	r.Lead = r.id
	nextIdx := r.RaftLog.LastIndex() + 1
	for _, item := range r.Prs {
		item.Next = nextIdx
		item.Match = 0
	}
	r.resetTick()
	// NOTE: Leader should propose a noop entry on its term
	entries := []*pb.Entry{{EntryType: pb.EntryType_EntryNormal}}
	err := r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgPropose, Entries: entries})
	if err != nil {
		log.Fatalf("fault to become leader, err : %s", err.Error())
	}
}

func (r *Raft) appendEntries(entries ...*pb.Entry) {
	r.RaftLog.appendEntriesAfter(entries...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		r.handleVote(&m)
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.campaign()
		}
	case pb.MessageType_MsgHeartbeat:
		if r.State != StateLeader {
			r.handleHeartbeat(&m)
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if r.State == StateLeader {
			r.handleHeartbeatResp(&m)
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.broadcastHeartbeat()
		}
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State == StateCandidate {
			r.handleVoteResp(&m)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgPropose:
		if r.State == StateLeader {
			lastIdx := r.RaftLog.LastIndex()
			entries := make([]*pb.Entry, 0)
			for _, e := range m.Entries {
				lastIdx++
				e.Term = r.Term
				e.Index = lastIdx
				if e.EntryType == pb.EntryType_EntryConfChange {
					if r.PendingConfIndex != None {
						continue
					}
					r.PendingConfIndex = e.Index
				}
				entries = append(entries, e)
			}
			r.appendEntries(entries...)
			r.broadcastAppend()
			r.commit()
		}
	case pb.MessageType_MsgAppendResponse:
		if r.State == StateLeader {
			r.handleAppendResp(&m)
		}
	default:
		return errors.New("unknown raft state")
	}
	return nil
}

func (r *Raft) handleHeartbeatResp(m *pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if r.isNewerLog(m.LogTerm, m.Index) {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleAppendResp(m *pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	if m.Reject {
		//TODO 可优化
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}

	r.Prs[m.From].Next = m.Index + 1
	r.Prs[m.From].Match = m.Index
	r.commit()
	//if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
	//	r.sendTimeoutNow(m.From)
	//}
}

func (r *Raft) commit() {
	halfNodeCount := len(r.Prs) >> 1
	i := r.RaftLog.committed + 1
	committedUpdated := false
	for i <= r.RaftLog.LastIndex() {
		matchCnt := 0
		for _, p := range r.Prs {
			if p.Match >= i {
				matchCnt++
			}
		}
		if halfNodeCount >= matchCnt {
			break
		}

		term, err := r.RaftLog.Term(i)
		if err != nil {
			log.Errorf("fault to get term, err: %s", err.Error())
			os.Exit(-1)
		}
		if term == r.Term {
			r.RaftLog.committed = i
			i = r.RaftLog.committed + 1
			committedUpdated = true
		} else {
			i++
		}
	}
	if committedUpdated {
		r.broadcastAppend()
	}

}

// bcastAppend is used by leader to bcast append request to followers
func (r *Raft) broadcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendAppendResp(m.From, true)
		return
	} else {
		r.becomeFollower(m.Term, m.From)
	}

	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResp(m.From, true)
		return
	}

	if len(m.Entries) > 0 {
		var i = 0
		for ; i < len(m.Entries); i++ {
			if m.Entries[i].Index > r.RaftLog.LastIndex() {
				break
			}
			term, err := r.RaftLog.Term(m.Entries[i].Index)
			if err != nil {
				log.Error(err)
				return
			}
			if term != m.Entries[i].Term {
				r.RaftLog.removeEntriesAfter(m.Entries[i].Index)
				break
			}
		}

		if i < len(m.Entries) && m.Entries[i].Index > r.RaftLog.LastIndex() {
			r.RaftLog.appendEntriesAfter(m.Entries[i:]...)
		}
	}

	if m.Commit > r.RaftLog.committed {
		lastIdx := m.Index
		if len(m.Entries) > 0 {
			lastIdx = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastIdx)
	}
	r.sendAppendResp(m.From, false)
}

func (r *Raft) sendSnap(to uint64) {
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snap,
	})
	r.Prs[to].Next = snap.Metadata.Index + 1
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
	//if r.RaftLog.committed < m.Commit {
	//	if r.RaftLog.LastIndex() < m.Commit {
	//		log.Fatalf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", m.Commit, r.RaftLog.LastIndex())
	//	}
	//	r.RaftLog.committed = m.Commit
	//}
	r.sendHeartbeatResp(m.From, true)
}

func (r *Raft) sendHeartbeatResp(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.LastIndex(),
	}
	term, err := r.RaftLog.Term(msg.Index)
	if err != nil {
		log.Fatalf(err.Error())
	}
	msg.LogTerm = term
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

func (r *Raft) handleVoteResp(m *pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		return
	}
	if m.Reject {
		r.votes[m.From] = false
		r.voteRejectCount++
	} else {
		r.votes[m.From] = true
		r.voteAgreeCount++
	}

	halfPeerCount := uint32(len(r.Prs)) >> 1
	if r.voteAgreeCount > halfPeerCount {
		r.becomeLeader()
	} else if r.voteRejectCount > halfPeerCount {
		r.becomeFollower(r.Term, None)
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

// 比较当前最后一条日志是否比传入的新，是返回true，否则返回false
func (r *Raft) isNewerLog(logTerm, index uint64) bool {
	lastIdx := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIdx)
	if err != nil {
		log.Fatalf("fault to log term, err: %s", err)
	}
	if lastTerm > logTerm || (lastTerm == logTerm && lastIdx > index) {
		return true
	}
	return false
}

// 处理选票请求
func (r *Raft) handleVote(m *pb.Message) {
	// 比对任期
	if r.Term > m.Term {
		r.sendVoteResp(m.From, true)
		return
	}

	if r.isNewerLog(m.LogTerm, m.Index) {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		r.sendVoteResp(m.From, true)
		return
	}

	if r.Term == m.Term && r.Vote != m.From && r.Vote != None {
		r.sendVoteResp(m.From, true)
		return
	}

	r.becomeFollower(m.Term, None)
	r.Vote = m.From
	r.sendVoteResp(m.From, false)
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

func (r *Raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}
