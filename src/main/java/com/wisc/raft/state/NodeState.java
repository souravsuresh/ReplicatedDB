package com.wisc.raft.state;

import com.wisc.raft.proto.Raft;
// import com.wisc.raft.log.LogEntry;
import com.wisc.raft.constants.Role;
import java.util.*;

public class NodeState {

    private String nodeId;
    private long currentTerm;
    private String votedFor;        // leader id?
    private List<Raft.LogEntry> entries;
    private Role nodeType;
    
    // volatile state on all servers
    private long commitIndex;    // index of highest log entry known to be commited 
    private long lastApplied;    // index of highest log entry applied to state machine

    //volatile state on leaders
    private List<Integer> nextIndex;    // for each server, index of next log entry to send to a particular server
                                        // initialized to leader logIndex+1
    private List<Integer> matchIndex;    // for each server, index of highest log entry known to be replicated on that particular server
                                        // initialized to 0
    private long leaderTerm;
    

    public NodeState(String nodeId) {
        this.currentTerm = 0;
        this.votedFor = null;
        this.entries = new ArrayList<>();

        this.commitIndex = 0;   
        this.lastApplied = 0;       // @CHECK :: should be -1

        this.nextIndex = new ArrayList<>();
        this.matchIndex = new ArrayList<>();

        this.nodeType = Role.FOLLOWER;
        this.leaderTerm = 0;     // @CHECK :: should be -1
        this.nodeId = nodeId;
    }
    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public List<Raft.LogEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<Raft.LogEntry> entries) {
        this.entries = entries;
    }
    public long getCommitIndex() {
        return commitIndex;
    }
    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }
    public long getLastApplied() {
        return lastApplied;
    }
    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }
    public List<Integer> getNextIndex() {
        return nextIndex;
    }
    public void setNextIndex(List<Integer> nextIndex) {
        this.nextIndex = nextIndex;
    }
    public List<Integer> getMatchIndex() {
        return matchIndex;
    }
    public void setMatchIndex(List<Integer> matchIndex) {
        this.matchIndex = matchIndex;
    }
    public Role getNodeType() {
        return nodeType;
    }
    public void setNodeType(Role nodeType) {
        this.nodeType = nodeType;
    }
    public long getLeaderTerm() {
        return leaderTerm;
    }
    public void setLeaderTerm(long leaderTerm) {
        this.leaderTerm = leaderTerm;
    }
    public String getNodeId() {
        return nodeId;
    }
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}