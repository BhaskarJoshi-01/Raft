package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"dissys/gob"
	"dissys/rpc"
)

// Raft is a Go structure implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex       // Lock to protect shared access to this peer's state
	peers     []*rpc.ClientEnd // RPC end points of all peers
	persister *Persister       // Object to hold this peer's persisted state
	me        int              // this peer's index into peers[]
	dead      int32            // set by Kill()
	size      int              // cluster size

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent status
	currentTerm int // init 0
	votedFor    int
	logs        []LogEntry
	offset      int

	// volatile status
	commitIndex      int
	lastAppliedIndex int

	// volatile status on Leader
	nextIndex  []int
	matchIndex []int

	// other states
	votes int // init 0

	// when a LogEntry is committed, it is immediately sent into applyChan
	applyChan chan ApplyMsg

	// when a LogEntry is requested, the append process is triggered by appendChan
	appendChan chan int

	// when Follower or Candidate receives AppendEntries from a valid Leader or grants a vote,
	// close the corresponding trigger and update timerStamp and timerState
	trigger *Trigger

	role int
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[NODE %d] term=%d,commitIdx=%d", rf.me, rf.currentTerm, rf.commitIndex)
}

// execute different processes based on the role of this Raft node.
func (rf *Raft) mainLoop() {

	for !rf.killed() {
		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		r := rf.role
		rf.mu.Unlock()
		/*-----------------------------------------*/
		switch r {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
	}
}

func (rf *Raft) followerLoop() {

	for !rf.killed() {
		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()

		// #region set a new Trigger and kick off
		timeout := (TimerBase + time.Duration(rand.Intn(TimerRange))) * time.Millisecond
		rf.trigger = NewTrigger()
		go rf.elapseTrigger(timeout, rf.trigger.StartTime)

		rf.mu.Unlock()

		// #endregion

		rf.trigger.Wait()

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		switch {
		case rf.trigger.Elapsed: // timer naturally elapses, turns to Candidate
			Debug(rf, "Follower turns to Candidate, timeout=%+v", timeout)
			rf.role = Candidate
			rf.trigger = nil
			rf.currentTerm++    // increment currentTerm
			rf.votedFor = rf.me // vote for self
			rf.votes = 1        // count # of votes
			rf.persist()
			rf.mu.Unlock()
			return
		default: // stays as Follower, set a new Trigger in the next round
			rf.role = Follower
			rf.trigger = nil
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/
	}
}

func (rf *Raft) candidateLoop() {

	for !rf.killed() {
		Debug(rf, "start new election")

		// region reset the timer for election end
		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		if rf.role != Candidate {
			rf.mu.Unlock()
			return
		}

		// set a new Trigger and kick off
		timeout := TimerBase*time.Millisecond + time.Duration(rand.Intn(TimerRange))*time.Millisecond
		rf.trigger = NewTrigger()
		// Debug(rf, "candidate timer=%+v, term=%d", timeout, rf.currentTerm)
		go rf.elapseTrigger(timeout, rf.trigger.StartTime)

		rf.mu.Unlock()
		/*-----------------------------------------*/
		// endregion

		for i := 0; i < rf.size; i++ {
			if i == rf.me {
				continue
			}

			go rf.sendRequestVote(i, rf.trigger.StartTime)
		}

		// Concurrency RequestVote RPCs

		rf.trigger.Wait()

		/*+++++++++++++++++++++++++++++++++++++++++*/
		// region :Judging whether the role changes in the next cycle
		rf.mu.Lock()
		rf.trigger = nil
		if rf.role != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.currentTerm++    // increment currentTerm
		rf.votedFor = rf.me // vote for self
		rf.votes = 1        // count # of votes
		rf.persist()
		rf.mu.Unlock()
		/*-----------------------------------------*/
		// endregion
	}
}

func (rf *Raft) leaderLoop() {

	// periodically send heartbeats
	/*-----------------------------------------*/
	for !rf.killed() {

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/

		// concurrently send heartbeats
		for i := 0; i < rf.size; i++ {
			if i == rf.me {
				continue
			}

			go rf.sendHeartBeat(i)
		}

		// after sending heartbeats, sleep Leader for a while
		sleeper := time.NewTimer(HeartBeatInterval * time.Millisecond)
		<-sleeper.C
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.offset)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm, votedFor, offset int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&offset) != nil {
		panic("BAD PERSIST")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.offset = offset
	}
}

//
// CondInstallSnapshot is a service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot is the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// SendRequestvote sends RPC requests and process RPC to return
// This function is asynchronous call
func (rf *Raft) sendRequestVote(server int, st int64) {

	rf.mu.Lock()
	// return is not candidate
	if rf.role != Candidate {
		rf.mu.Unlock()
		return
	}

	var req RequestVoteRequest
	var resp RequestVoteResponse
	// Get the last LOG ENTRY information
	entry, _ := rf.lastLogInfo()

	req = RequestVoteRequest{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  entry.Index,
		LastLogTerm:   entry.Term,
	}

	rf.mu.Unlock()

	// Send an RPC request.When not OK, it means that the network is abnormal.
	if ok := rf.peers[server].Call("Raft.RequestVoteHandler", &req, &resp); !ok {
		resp.Info = NetworkFailure
	}

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// When RPC returns, it may have been returned to the Follower or selected as a leader.
	if rf.role != Candidate {
		return
	}

	switch resp.Info {
	case Granted: // Vote

		// When you get this vote, your term has been updated, and the votes are invalid.
		if rf.currentTerm != req.CandidateTerm {
			return
		}

		rf.votes++

		if rf.votes > rf.size/2 { // Most votes that get the current term. become leader
			// Has become a leader
			if rf.role == Leader {
				return
			}

			// Selected leader
			rf.role = Leader
			Debug(rf, "#####LEADER ELECTED! votes=%d, Term=%d#####", rf.votes, rf.currentTerm)

			// reinitialize volatile status after election
			// Empty log back index 0
			entry, _ := rf.lastLogInfo()
			lastLogIndex := entry.Index

			// When elected, automatically fill in an empty logentry
			// lastLogIndex++
			// rf.logs = append(rf.logs, LogEntry{
			// 	Index: lastLogIndex,
			// 	Term:  rf.currentTerm,
			// })
			// rf.persist()

			for i := 0; i < rf.size; i++ {
				// nextIndex[]: initialize to leader last logger index + 1
				// Initialization is 1, the minimum will not be less than 1
				rf.nextIndex[i] = lastLogIndex + 1

				// matchIndex[]: initialize to 0
				rf.matchIndex[i] = 0
			}

			rf.matchIndex[rf.me] = lastLogIndex

			Debug(rf, "Upon election, match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)

			// End timer
			rf.closeTrigger(st)

		}

	case TermOutdated: // The term of office when sending RPC expires
		// Maybe the current term is the latest
		if rf.currentTerm >= resp.ResponseTerm {
			return
		}

		// Update term, return to Follower
		rf.currentTerm = resp.ResponseTerm
		rf.role = Follower
		rf.persist()
		Debug(rf, "term is out of date and roll back, %d<%d", rf.currentTerm, resp.ResponseTerm)

		// End timer
		rf.closeTrigger(st)

	case Rejected:
		Debug(rf, "VoteRequest to server %d is rejected", server)

	case NetworkFailure:
		Debug(rf, "VoteRequest to server %d timeout", server)
	}
	/*-----------------------------------------*/
}
