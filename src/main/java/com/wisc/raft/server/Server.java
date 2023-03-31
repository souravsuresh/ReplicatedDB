package com.wisc.raft.server;

import java.util.*;
import com.wisc.raft.constants.Role;
import com.wisc.raft.constants.CommandType;
import com.wisc.raft.state.NodeState;
import com.wisc.raft.proto.Raft;


public class Server {

    protected NodeState state;

    private double electionTimeout;     // required by followers to know when they can become future leaders :-)
    private double heartbeatTimeout;    // required for leader to send Append entries
    private double statemachineTimeout; // required to trigger state machine orchastrator

    private static final Random random = new Random();
    private static final long ELECTION_TIMEOUT_INTERVAL = 100;  //100ms
    private static final long MAX_REQUEST_RETRY = 3;  // retry requests for 3 times

    // @TODO: for now maintaining HashMap!! Update this to db updates
    private HashMap<Long, Long> kvStore = new HashMap<>();

    public Server(String nodeId) {
        this.state = new NodeState(nodeId);
    }

    // @TODO :: complete the function with mentioned todo's
    private void resetElectionTimeout(){
        // @TODO:: cancel all the scneduled futures election
        electionTimeout = ELECTION_TIMEOUT_INTERVAL + random.nextDouble() * ELECTION_TIMEOUT_INTERVAL;
        // @TODO :: schedule new Futures
    }
    

    // @TODO :: called by the threads once timeout happens (ensure this!!)
    private void handleElectionTimeout() {
        this.state.setNodeType(Role.CANDIDATE);
        
        initiateRequestVoteRPC();
    }

    // @TODO :: Complete this!!
    private void initiateRequestVoteRPC() {
        /*
         * @TODO
         * 1. Increment currentTerm
         * 2. vote for self
         * 3. reset election timer
         * 4. send parallely vote request to other server (if state.id != server.id)
         * 5. Aggregate the results
         * 6. If election time out happens just reinitiate (THIS WOULD BE HANDLED from Step 3)
         * 7. while waiting for results/ before validate if we got Append RPC (code has been adeded)
         * wrt response check if its eligible to become leader
         */

        // increment current term
        this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
        // voting for self
        this.state.setVotedFor(this.state.getNodeId());

        this.resetElectionTimeout();

        // @TODO : send Request vote RPC all servers (extend with threads to all server)
        Raft.RequestVote.Builder requestBuilder = Raft.RequestVote.newBuilder();
        requestBuilder.setCandidateId(this.state.getNodeId());
        requestBuilder.setTerm(this.state.getCurrentTerm());
        requestBuilder.setLastLogIndex(this.state.getCommitIndex());

        long lastLogTerm = this.state.getCommitIndex() == 0? 0 : this.state.getEntries().get(this.state.getEntries().size() - 1).getTerm();
        requestBuilder.setLastLogTerm(lastLogTerm);
        
        Raft.RequestVote request = requestBuilder.build();

        // @TODO : call the server with above request object!1

        // @CHECK :: add locks
        if(this.state.getNodeType() != Role.CANDIDATE) {
            System.out.println("Some other guy took over the leadership!! ");
        } else { 
            // Accumulate results and update the state (terms/ curretLeader/ logEntries etc from RPC resp) 
        }
    }

    // @TODO send as Actual RPC response (write a wrapper for this!!)
    public Raft.AppendEntriesResponse AppendEntries(Raft.AppendEntriesRequest leader) {

        Raft.AppendEntriesResponse.Builder responseBuilder = Raft.AppendEntriesResponse.newBuilder();

        responseBuilder.setTerm(this.state.getCurrentTerm());
        responseBuilder.setSuccess(false);
        // return if term < currentTerm
        if (leader.getTerm() < this.state.getCurrentTerm()) {
            return responseBuilder.build();     
        }
        
        // 2nd condition can be possible when current node was voting while it got AppendRPC
        if (leader.getTerm() > this.state.getCurrentTerm() ||
             this.state.getVotedFor().equals(this.state.getNodeId())) {
        
            //update currentTerm
            this.state.setCurrentTerm(leader.getTerm());
            this.state.setLeaderTerm(leader.getTerm()); 
        
            //step down if leader or candidate
            if (this.state.getNodeType() != Role.FOLLOWER) {
                this.state.setNodeType(Role.FOLLOWER);
            }
            //reset election timeout
            resetElectionTimeout();
        }

        //return failure if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if (this.state.getEntries().size() < leader.getPrevLogIndex() 
            || leader.getPrevLogTerm() == this.state.getEntries().get((int)leader.getPrevLogIndex()).getTerm()) {
            return responseBuilder.build(); 
        }

        boolean conflictDeleted = false;
        // NOTE: this started from 0 but adding lastApplied since thats the last applied/ commited state
        long index = this.state.getLastApplied();
        for (; index < leader.getEntriesList().size(); index++) {
            Raft.LogEntry newEntry = leader.getEntries((int) index);
            long indexOnServer = leader.getPrevLogIndex() + 1 + index;
        
            //if existing entries conflict with new entries
            if (!conflictDeleted 
                && this.state.getEntries().size() >= indexOnServer 
                && !this.state.getEntries().get((int)indexOnServer).equals(newEntry)) {
                //delete all existing entries starting with first conflicting entry
                for(long j = indexOnServer; j < this.state.getEntries().size(); ++j){
                    this.state.getEntries().remove((int) j);
                }
                conflictDeleted = true;
            }
            //append any new entries not already in the log
            this.state.getEntries().add(newEntry);
        }
        
        if(leader.getCommitIndex() > this.state.getCommitIndex()) {
            this.state.setCommitIndex(Math.min(leader.getCommitIndex(), index));
        }

        //@TODO: check advance state machine with newly committed entries (Execute set/ put calls?)
        responseBuilder.setSuccess(true);
        return responseBuilder.build(); 

    }
    
    public Raft.ResponseVote recieveRequestVote(Raft.RequestVote candidate) {
        boolean voted = false;
        Raft.ResponseVote.Builder responseBuilder = Raft.ResponseVote.newBuilder();
        
        //if term > currentTerm
        if (candidate.getTerm() > this.state.getCurrentTerm()) {
            
            // set the current term to the candidates term 
            this.state.setCurrentTerm(candidate.getTerm());

            //step down if leader or candidate
            if (this.state.getNodeType() != Role.FOLLOWER) {
                this.state.setNodeType(Role.FOLLOWER);
            }
        }
        
        //if term = currentTerm, voteFor is null or candidateId, and candidate's log is at least as complete as local log
        // @CHECK :: We need add one more check of (candidate.getLastLogIndex() >= this.state.getlastEntry().getIndex())
        if (candidate.getTerm() == this.state.getCurrentTerm() 
                && (this.state.getVotedFor() == null || this.state.getVotedFor().equals(candidate.getCandidateId()))) {
            //grant vote
            voted = true;
            // @Check :: whether we need to add node UUID here?
            this.state.setVotedFor(candidate.getCandidateId());
        }
        
        responseBuilder.setGrant(voted);
        responseBuilder.setTerm(this.state.getCurrentTerm());
        resetElectionTimeout();

        return responseBuilder.build();
    }

    //@TODO:: Complete Scheduled state machine orchastrator (add a ScheduledThread?)
    protected void stateMachineOrchastrator() {
        if(this.state.getCommitIndex() > this.state.getLastApplied()) {
            List<Raft.LogEntry> entry = this.state.getEntries();
            for(int i = (int) this.state.getLastApplied(); i < this.state.getCommitIndex(); ++i) {
                Raft.Command command = entry.get(i).getCommand();
                if(command.getCommandType().equals(CommandType.PUT.toString())) {
                    // @TODO Add entry to database
                    this.kvStore.put(command.getKey(), command.getValue());
                    this.state.setLastApplied(this.state.getLastApplied() + 1);
                } 
                // else if(command.getCommandType() == CommandType.HEARTBEAT) {
                //     this.resetElectionTimeout();
                // }
                // @CHECK :: Should we handle anything for get?
            }
            
            //@CHECK :: Optionall
            if(this.state.getCurrentTerm() > this.state.getLeaderTerm()) {
                //@TODO :: Possibality of NW Partition ?
                System.out.println("Node Current Term" + this.state.getCurrentTerm() + "is greater than leader term " + this.state.getLeaderTerm());
            } else if(this.state.getLeaderTerm() > this.state.getCurrentTerm()) {
                this.state.setCurrentTerm(this.state.getLeaderTerm());
            } else {
                System.out.println("NodeTerm Check passed!!");
            }
            //ensure lastApplied is updated
        }
    }

    // @TODO :: this is scheduled based on heartbeat timeout!!
    private void heartbeatOrchastrator() {
         /* @TODO
         * FOR Leader: If last log index ≥ nextIndex for a follower: send
                        AppendEntries RPC with log entries starting at nextIndex
                        • If successful: update nextIndex and matchIndex for
                        follower (§5.3)
                        • If AppendEntries fails because of log inconsistency:
                        decrement nextIndex and retry (§5.3)
                        • If there exists an N such that N > commitIndex, a majority
                        of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                        set commitIndex = N (§5.3, §5.4)
         */

         // initiate Append RPC call to all followers with above mentioned algo!
         // To start with, just Append RPC with command type heartbeat!! 
         //         expand further with above algo for consistecy!

    }

    private void sendAppendEntries() {
        Raft.AppendEntriesRequest.Builder requestBuilder = Raft.AppendEntriesRequest.newBuilder();
        requestBuilder.setLeaderId(this.state.getNodeId());
        requestBuilder.setTerm(this.state.getCurrentTerm());
        requestBuilder.setPrevLogIndex(this.state.getCommitIndex() == 0?0:this.state.getCommitIndex()-1);
        
        // @CHECK :: Snapshot might mess this up!! Possibility is this.state.term
        int delta = 0;
        int size = this.state.getEntries().size();

        // @CHECK :: Ideally the lastIndex and lastTerm should be tracked (Cases when multiple entrues are added)
        if(size >  1) {
            delta += 1; 
        } 
        if(!this.state.getEntries().get(size - 1).getCommand().getCommandType().equals(CommandType.HEARTBEAT.toString())) {
            delta += 1;
        }
        long lastLogTerm = size == 0 ? 0 : this.state.getEntries().get(this.state.getEntries().size() - delta).getTerm();
        requestBuilder.setPrevLogTerm(lastLogTerm);
        requestBuilder.addAllEntries(this.state.getEntries());
        // Raft.AppendEntriesRequest = requestBuilder.build();
        
        //@TODO :: Add service to send the request

    }

    // @TODO change inp and return params to RPC types
    public long get(long key) {
        if(this.state.getNodeType() != Role.LEADER) {
            System.out.println("Cant perform action as this is not leader!!");
            return -1;
        }
        if(!this.kvStore.containsKey(key)) {
            //return RPC resp of key not found
            System.out.println("Key Not Found  :: "+key);
            return -1;
        } 
        // model this has RPC Response and change return type acc
        return this.kvStore.get(key);
    }

    // @TODO change inp and return params to RPC types and complete this
    public int put(int key, int val) {
        /*
         * 1. append to log entry
         * 2. increment log index
         * 3. issue append rpc in parallel to other servers (this.state.nodeid != serverId)
         * 4. 
         */
        if(this.state.getNodeType() != Role.LEADER) {
            System.out.println("Cant perform action as this is not leader!!");
            return -1;
        }


        Raft.Command command = Raft.Command.newBuilder().
                                setKey(key).setValue(val).
                                setCommandType(CommandType.PUT.toString()).build();
        
        Raft.LogEntry entry = Raft.LogEntry.newBuilder().
                                setCommand(command).
                                setTerm(this.state.getCurrentTerm()).setIndex(this.state.getNodeId()).build();

        this.state.getEntries().add(entry);
        //  @TODO :: Issue appendEntries RPC call
        // if majority of followers send "true" appendRPC response
        //      commit the entry (refer below code at line # 204)
        // else
        //      probably retry for MAX_REQUEST_RETRY times?
        this.state.setCommitIndex(this.state.getCommitIndex()+1);
        this.stateMachineOrchastrator();

        // If we reach here we can send SUCCESS RPC to client
        return 1;
    }
}
