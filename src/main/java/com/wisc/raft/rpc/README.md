

---
### RequestVote RPC (Invoked by candidates to gather votes)
----

Request:
- term: candidate’s term
- candidateId: candidate requesting vote
- lastLogIndex: index of candidate’s last log entry
- lastLogTerm: term of candidate’s last log entry

Response:
- term: currentTerm, for candidate to update itself
- voteGranted: true means candidate received vote

RPC call implementation:
1. Reply false if term < currentTerm
2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote


---
### Append RPC (Invoked by leader to replicate log entries)
----

Request:
- term:  leader’s term 
- leaderId: so follower can redirect clients
- prevLogIndex: index of log entry immediately preceding new ones
- prevLogTerm: term of prevLogIndex entry
- entries[]: log entries to store (empty for heartbeat; may send more than one for efficiency)
- leaderCommit: leader’s commitIndex

Results:
- term: currentTerm, for leader to update itself
- success: true if follower contained entry matching prevLogIndex and prevLogTerm)


RPC implementation:
1. Reply false if term < currentTerm 
2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm 
3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it 
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

