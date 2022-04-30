package raft

import (
	"sync/atomic"
	"time"
)

type Trigger struct {
	On        bool
	C         chan int
	StartTime int64
	Elapsed   bool
}

func NewTrigger() *Trigger {
	return &Trigger{
		StartTime: time.Now().UnixNano(),
		C:         make(chan int),
		On:        true,
	}
}

func (t *Trigger) Wait() {
	<-t.C
}

func (t *Trigger) Elapse() {
	t.Elapsed = true
	t.On = false
	close(t.C)
}

func (t *Trigger) Close() {
	t.On = false
	close(t.C)
}

// Trigger naturally elapses
func (rf *Raft) elapseTrigger(d time.Duration, st int64) {
	time.Sleep(d)
	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// When the timer triggered at the moment is still, it will expire
	if rf.trigger != nil && rf.trigger.On && rf.trigger.StartTime == st {
		rf.trigger.Elapse()
	}
	/*-----------------------------------------*/
}

// close the Trigger in advance
// caller is within a critical section, no need to lock
func (rf *Raft) closeTrigger(st int64) {
	if rf.trigger != nil && rf.trigger.On && rf.trigger.StartTime == st {
		rf.trigger.Close()
	}
}

// reset the Trigger
// caller is within a critical section, no need to lock
func (rf *Raft) resetTrigger() {
	if rf.trigger != nil && rf.trigger.On {
		rf.trigger.Close()
	}
}

func (rf *Raft) printLog() {
	Debug(rf, "AppendEntries returns")
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Lastloginfo returns the last log line information.
// The caller is not locked in the critical area.
// If the log is empty, return to Zerology.
func (rf *Raft) lastLogInfo() (LogEntry, bool) {

	if len(rf.logs) == 0 {
		return ZeroLogEntry, false
	}

	return rf.logs[len(rf.logs)-1], true
}
