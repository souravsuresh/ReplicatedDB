package com.wisc.raft.server;

import com.wisc.raft.constants.CommandType;
import com.wisc.raft.constants.Role;
import com.wisc.raft.dto.PeerInfo;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.RaftServiceGrpc;
import com.wisc.raft.service.RaftConsensusService;
import com.wisc.raft.state.NodeState;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class Server {

    static int count = 0;
    static int msg = 0;
    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = state;
    }

    protected NodeState state;

    private double electionTimeout;     // required by followers to know when they can become future leaders :-)
    private double heartbeatTimeout;    // required for leader to send Append entries
    private double statemachineTimeout; // required to trigger state machine orchastrator


    private static final Random random = new Random();
    private static final long ELECTION_TIMEOUT_INTERVAL = 100;  //100ms
    private static final long MAX_REQUEST_RETRY = 3;  // retry requests for 3 times

    // @TODO: for now maintaining HashMap!! Update this to db updates
    private HashMap<Long, Long> kvStore = new HashMap<>();
    private HashMap<Long, PeerInfo> peerInfo = new HashMap<>();

    private List<Raft.ServerConnect> cluster;

    //Threading
    private ExecutorService executorService; //Generic submitter
    private ScheduledExecutorService scheduledExecutorService;

    private ScheduledExecutorService schNodeEntries;

    private ScheduledFuture electionScheduler;
    private ScheduledFuture heartBeatScheduler;


    private RaftConsensusService raftConsensusService;

    public Server(String nodeId) {

        this.state = new NodeState(nodeId);
    }

    // @TODO :: complete the function with mentioned todo's
    public List<Raft.ServerConnect> getCluster() {
        return cluster;
    }

    public void setCluster(List<Raft.ServerConnect> custer) {
        this.cluster = custer;
    }

    public void start() throws InterruptedException {

        //TODO : Loop - fill the map
        List<Integer> matchIndex =  new ArrayList<>();
        List<Integer> nextIndex = new ArrayList<>();
        System.out.println("CLuster size : " + cluster.size());
        cluster.stream().forEach(serv -> {
            matchIndex.add(-1);
            nextIndex.add(0);
        });
        this.state.setNextIndex(nextIndex);
        this.state.setMatchIndex(matchIndex);

        //Defining threading
        executorService = new ThreadPoolExecutor(4, 4, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        scheduledExecutorService = Executors.newScheduledThreadPool(4);
        schNodeEntries = Executors.newScheduledThreadPool(1);
//        schNodeEntries.scheduleWithFixedDelay(new Runnable() {
//            @Override
//            public void run() {
//                addToEntries();
//            }
//        }, 1, 1, TimeUnit.MINUTES);

        resetElectionTimeout();
    }

    private void addToEntries(){
        Raft.Command cmd = Raft.Command.newBuilder().setCommandType("cmd").setKey(msg).setValue(msg).build();
        this.state.getEntries().add(Raft.LogEntry.newBuilder().setIndex(String.valueOf(msg)).setCommand(cmd).setTerm(this.state.getCurrentTerm()).build());
        msg++;
    }

    private void resetElectionTimeout() throws InterruptedException {
        System.out.println("[ResetElectionTimeout] Inside reset election timeout!!");
        // @TODO:: cancel all the scheduled futures election
        if(this.state.getNodeType().equals(Role.LEADER)) {
            System.out.println("[ResetElectionTimeout]  Already a leader! So not participating in Election!");
            System.out.println(Thread.currentThread().getName());
            electionScheduler.cancel(true);
            System.out.println(Thread.currentThread().getName() + " HEllo World ");
            return;
        }
        if (electionScheduler != null && !electionScheduler.isDone()) {
            electionScheduler.cancel(true);
        }
        count = 0;
        this.state.setVotedFor(null);
        electionTimeout = 100 + random.nextDouble() * ELECTION_TIMEOUT_INTERVAL;
        System.out.println("[ResetElectionTimeout] Inside resetElectionTimeout " + " : " +  electionTimeout);
        this.state.setNodeType(Role.CANDIDATE);

        electionScheduler = scheduledExecutorService.schedule(() -> {
            try {
                initiateRequestVoteRPC();
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("HELP ME"+ e);
                throw new RuntimeException(e);
            }
        }, 20, TimeUnit.SECONDS);   // @TODO :: change to milliseconds back
    }

    // @TODO :: Complete this!!
    private void initiateRequestVoteRPC() throws InterruptedException, ExecutionException {
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



        System.out.println(this.state.getNodeType() + " : " + Objects.isNull(this.state.getVotedFor()));
        if(!this.state.getNodeType().equals(Role.CANDIDATE) && !Objects.isNull(this.state.getVotedFor())){
            return;
        }
        // increment current term
        System.out.println("[InitiateVote] Inside initiateRequestVoteRPC  !!");
        this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
        // voting for self
        this.state.setVotedFor(this.state.getNodeId());
        // @TODO : send Request vote RPC all servers (extend with threads to all server)
        Raft.RequestVote.Builder requestBuilder = Raft.RequestVote.newBuilder();
        requestBuilder.setCandidateId(this.state.getNodeId());
        requestBuilder.setTerm(this.state.getCurrentTerm());
        requestBuilder.setLastLogIndex(this.state.getCommitIndex());
        count++;
        long lastLogTerm = this.state.getCommitIndex() == 0 ? 0 : this.state.getEntries().get(this.state.getEntries().size() - 1).getTerm();
        requestBuilder.setLastLogTerm(lastLogTerm);

        Raft.RequestVote request = requestBuilder.build();

        // @TODO : call the server with above request object!1
        List<Callable<Integer>> todo = new ArrayList<>();
        cluster.stream().forEach(serv -> {
            if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                Callable runnableTask = () -> {

                    return requestVote(request, serv.getEndpoint());
                };

                todo.add(runnableTask);
            }

        });
        List<Future<Integer>> futures = executorService.invokeAll(todo);
        System.out.println("[InitiateVote] Waiting for all threads to complete");
        for (int i = 0; i < futures.size(); i++) {
            try {
                Integer future = futures.get(i).get();
            } catch (Exception e) {
                System.out.println("[InitiateVote] Exception :: " +e);
            }
        }
        System.out.println("[InitiateVote] All threads completed Execution");

        System.out.println("[InitiateVote] Voting Results:  NodeType : " +this.state.getNodeType() +  " Votes : " + count);


        // @CHECK :: add locks
        if (this.state.getNodeType() != Role.CANDIDATE) {
            System.out.println("[InitiateVote] Some other guy took over the leadership!! ");
        } else if(count < cluster.size()/2 + 1) {
            System.out.println("[InitiateVote] Dit not get majority :: Current Votes -> "+ count + ", Expected Votes =>" + cluster.size()/2);
            this.resetElectionTimeout();
        }
        else{
            System.out.println("[InitiateVote] Got the leadership ::  NodeType : " +this.state.getNodeType() +  " Votes : " + count + " current term: " + this.state.getCurrentTerm());
            this.state.setNodeType(Role.LEADER);
            // stop vote timer
//            if (electionScheduler != null && !electionScheduler.isDone()) {
//                electionScheduler.cancel(true);
//            }
            // start heartbeat timer

            startHeartBeatAsLeader();
        }
    }

    private void startHeartBeatAsLeader() throws InterruptedException {
        List<Callable<Integer>> todo = new ArrayList<>();
        System.out.println("Inside startHeartBeatAsLeader");
        cluster.stream().forEach(serv -> {
            if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                Callable runnableTask = () -> {
                    sendAppendEntries(serv);
                    return 1;
                };
                todo.add(runnableTask);
            }
        });
        List<Future<Integer>> futures = executorService.invokeAll(todo);
        System.out.println(futures.size() +  " : size of future object FML as HBL" );
        System.out.println("[startHeartBeatAsLeader] Waiting for all threads to complete");
        for (int i = 0; i < futures.size(); i++) {
            try {
                Integer future = futures.get(i).get();
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("[startHeartBeatAsLeader] Exception :: " +e);
                e.printStackTrace();
            }
        }
        System.out.println("Done with one heartBeat");
        System.out.println("Match_Index : " +  this.state.getMatchIndex());
        System.out.println("Next_Index : " + this.state.getNextIndex());


        //Also start the scheduled version of the same
        scheduleHeartBeatTimer();

    }

    private void scheduleHeartBeatTimer() {
        if (heartBeatScheduler != null && !heartBeatScheduler.isDone()) {
            heartBeatScheduler.cancel(true);
        }
        heartBeatScheduler = scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    startHeartBeatAsLeader();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 60, TimeUnit.SECONDS);
    }


    //TODO PEER param
    private int requestVote(Raft.RequestVote request, Raft.Endpoint endpoint) {
        System.out.println("[RequestVoteWrapper] Inside requestVote for endpoint :: "+ endpoint.getPort());

        ManagedChannel channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort())
                .usePlaintext()
                .build();

        System.out.println("[RequestVoteWrapper] Channel state :: "+ channel.getState(true) + " :: "+ channel.getState(false));
        if (channel.getState(false) == ConnectivityState.TRANSIENT_FAILURE){
            System.out.println("Follower is down!! "+ endpoint.getPort());
            return -1;
        }
        System.out.println("[RequestVoteWrapper] CandidateID : " + request.getCandidateId());
        RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);

        Raft.ResponseVote responseVote = raftServiceBlockingStub.requestVotes(request);
        if (responseVote.getGrant()) {
            //What if he became leader
            if (this.state.getCurrentTerm() != responseVote.getTerm() || this.state.getNodeType() != Role.CANDIDATE) {
                System.out.println("[RequestVoteWrapper] The response term is mismatched or Current node state is not candidate :: "+this.state.getNodeType());
                return 0;
            }

            if (responseVote.getTerm() > this.state.getCurrentTerm()) {
                System.out.println("[RequestVoteWrapper] Received term response from other follower :: "+ responseVote.getTerm() +" more than current term ::" +this.state.getCurrentTerm());

                //stepDown(response.getTerm());
                return 0;
            }
            else{
                count++;
                return 1;
            }
        } else {
            System.out.println("[RequestVoteWrapper] Not granted by :: " + endpoint.getPort() + " Response :: "+ responseVote.getTerm() + " Current :: " + this.state.getCurrentTerm());
        }
        return 0;

    }

    // @TODO send as Actual RPC response (write a wrapper for this!!)
    public Raft.AppendEntriesResponse AppendEntries(Raft.AppendEntriesRequest leader) throws InterruptedException {

        Raft.AppendEntriesResponse.Builder responseBuilder = Raft.AppendEntriesResponse.newBuilder();

        responseBuilder.setTerm(this.state.getCurrentTerm());
        responseBuilder.setSuccess(false);
        // return if term < currentTerm
        if (leader.getTerm() < this.state.getCurrentTerm()) {
            return responseBuilder.build();
        }

        // 2nd condition can be possible when current node was voting while it got AppendRPC
        if (leader.getTerm() > this.state.getCurrentTerm() || this.state.getVotedFor().equals(this.state.getNodeId())) {

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
        if (this.state.getEntries().size() < leader.getPrevLogIndex() || leader.getPrevLogTerm() == this.state.getEntries().get((int) leader.getPrevLogIndex()).getTerm()) {
            return responseBuilder.build();
        }

        boolean conflictDeleted = false;
        // NOTE: this started from 0 but adding lastApplied since thats the last applied/ commited state
        long index = this.state.getLastApplied();
        for (; index < leader.getEntriesList().size(); index++) {
            Raft.LogEntry newEntry = leader.getEntries((int) index);
            long indexOnServer = leader.getPrevLogIndex() + 1 + index;

            //if existing entries conflict with new entries
            if (!conflictDeleted && this.state.getEntries().size() >= indexOnServer && !this.state.getEntries().get((int) indexOnServer).equals(newEntry)) {
                //delete all existing entries starting with first conflicting entry
                for (long j = indexOnServer; j < this.state.getEntries().size(); ++j) {
                    this.state.getEntries().remove((int) j);
                }
                conflictDeleted = true;
            }
            //append any new entries not already in the log
            this.state.getEntries().add(newEntry);
        }

        if (leader.getCommitIndex() > this.state.getCommitIndex()) {
            this.state.setCommitIndex(Math.min(leader.getCommitIndex(), index));
        }

        //@TODO: check advance state machine with newly committed entries (Execute set/ put calls?)
        responseBuilder.setSuccess(true);
        return responseBuilder.build();

    }

    public Raft.ResponseVote recieveRequestVote(Raft.RequestVote candidate) throws InterruptedException {
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
        if (candidate.getTerm() == this.state.getCurrentTerm() && (this.state.getVotedFor() == null || this.state.getVotedFor().equals(candidate.getCandidateId()))) {
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
        if (this.state.getCommitIndex() > this.state.getLastApplied()) {
            List<Raft.LogEntry> entry = this.state.getEntries();
            for (int i = (int) this.state.getLastApplied(); i < this.state.getCommitIndex(); ++i) {
                Raft.Command command = entry.get(i).getCommand();
                if (command.getCommandType().equals(CommandType.PUT.toString())) {
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
            if (this.state.getCurrentTerm() > this.state.getLeaderTerm()) {
                //@TODO :: Possibality of NW Partition ?
                System.out.println("Node Current Term" + this.state.getCurrentTerm() + "is greater than leader term " + this.state.getLeaderTerm());
            } else if (this.state.getLeaderTerm() > this.state.getCurrentTerm()) {
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

    private void sendAppendEntries(Raft.ServerConnect server) {

        System.out.println("sendAppendEntries : Inside " + server.getServerId());
        Raft.AppendEntriesRequest.Builder requestBuilder = Raft.AppendEntriesRequest.newBuilder();
        if(this.state.getNodeType() != Role.LEADER){
            System.out.println("sendAppendEntries : I can't do this");
            return;
        }
        // @CHECK :: Snapshot might mess this up!! Possibility is this.state.term
        int delta = 0;
        int size = this.state.getEntries().size();
        int followerIndex = this.state.getNextIndex().get(server.getServerId());
        long peerMatchIndex = this.state.getMatchIndex().get(server.getServerId()); //-1
        long currentIndex = this.state.getCommitIndex();
        long currentTerm  = this.state.getCurrentTerm();
        if(currentIndex < followerIndex){
            System.out.println("This cannot happen right ??");
            return;
        }

        //Check last match Index and send everything from there to current irrespective if it is HB
        requestBuilder.setTerm(state.getCurrentTerm()); // Is this correct ?
        requestBuilder.setLeaderId(this.state.getNodeId());
        if(this.state.getEntries().isEmpty()){
            //Do something
        }
        if(followerIndex == 0){
            requestBuilder.setPrevLogTerm(0);
            requestBuilder.setPrevLogIndex(-1);
        }
        else {
            requestBuilder.setPrevLogTerm(this.state.getEntries().get(followerIndex - 1).getTerm());
            requestBuilder.setPrevLogIndex(followerIndex - 1);

        }
        requestBuilder.setCommitIndex(this.state.getCommitIndex()); // Last Commit Index
        List<Raft.LogEntry> entries = this.state.getEntries();
        List<Raft.LogEntry> entryToSend = new ArrayList<>();

        //ConvertToInt
        for(long i = peerMatchIndex+1; i<entries.size(); i++){
            entryToSend.add(entries.get((int) i));
        }
        requestBuilder.addAllEntries(entryToSend);

        Raft.AppendEntriesRequest build = requestBuilder.build();
        System.out.println("sendAppendEntries : before call : " + server.getServerId());

        ManagedChannel channel = ManagedChannelBuilder.forAddress(server.getEndpoint().getHost(), server.getEndpoint().getPort())
                .usePlaintext()
                .build();
        RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);

        Raft.AppendEntriesResponse response =  raftServiceBlockingStub.appendEntries(build);

        //TODO check for exception
        boolean success = response.getSuccess();
        if(!success) {
            long term = response.getTerm();
            if (term == -1) {
                System.out.println("Got rejected, as my term was lower as a leader. This shouldn't be happening");
            }

            if (term == -2) {
                System.out.println("Follower thinks someone else is leader");
            }

            if (term == -3) {
                System.out.println("We have different prev index, this shouldn't happen in this design , but can happen in future");
            }

            if (term == -4) {
                System.out.println("We have different term is not corrected : self correct Ig");
            }
            return;
        }
        else{
            System.out.println("Post Response");
            if(entryToSend.size() == 0){
                return;
            }
            this.state.getNextIndex().set(server.getServerId(), (int) currentIndex + 1);
            this.state.getMatchIndex().set(server.getServerId(),(int) currentIndex);

        }


        // @CHECK :: Ideally the lastIndex and lastTerm should be tracked (Cases when multiple entrues are added)

//        if (!this.state.getEntries().get(size - 1).getCommand().getCommandType().equals(CommandType.HEARTBEAT.toString())) {
//
//            delta += 1;
//        }

//        check for exception {
//
//        }
//        long lastLogTerm = size == 0 ? 0 : this.state.getEntries().get(this.state.getEntries().size() - delta).getTerm();
//
//        requestBuilder.setPrevLogTerm(lastLogTerm);
//        requestBuilder.addAllEntries(this.state.getEntries());
//        // Raft.AppendEntriesRequest = requestBuilder.build();

        //@TODO :: Add service to send the request

    }

    public void downGrade(Role role){
        this.getState().setNodeType(role);
        if (heartBeatScheduler != null && !heartBeatScheduler.isDone()) {
            heartBeatScheduler.cancel(true);
        }
        if (electionScheduler != null && !electionScheduler.isDone()) {
            electionScheduler.cancel(true);
        }

    }
    // @TODO change inp and return params to RPC types
    public long get(long key) {
        if (this.state.getNodeType() != Role.LEADER) {
            System.out.println("Cant perform action as this is not leader!!");
            return -1;
        }
        if (!this.kvStore.containsKey(key)) {
            //return RPC resp of key not found
            System.out.println("Key Not Found  :: " + key);
            return -1;
        }
        // model this has RPC Response and change return type acc
        return this.kvStore.get(key);
    }

    // @TODO change inp and return params to RPC types and complete this
    public int put(long key, long val) {
        /*
         * 1. append to log entry
         * 2. increment log index
         * 3. issue append rpc in parallel to other servers (this.state.nodeid != serverId)
         * 4.
         */
        if (this.state.getNodeType() != Role.LEADER) {
            System.out.println("Cant perform action as this is not leader!!");
            return -1;
        }


        Raft.Command command = Raft.Command.newBuilder().setKey(key).setValue(val).setCommandType(CommandType.PUT.toString()).build();

        Raft.LogEntry entry = Raft.LogEntry.newBuilder().setCommand(command).setTerm(this.state.getCurrentTerm()).setIndex(this.state.getNodeId()).build();

        this.state.getEntries().add(entry);
        //  @TODO :: Issue appendEntries RPC call
        // if majority of followers send "true" appendRPC response
        //      commit the entry (refer below code at line # 204)
        // else
        //      probably retry for MAX_REQUEST_RETRY times?
        this.state.setCommitIndex(this.state.getCommitIndex() + 1);
        this.stateMachineOrchastrator();

        // If we reach here we can send SUCCESS RPC to client
        return 1;
    }
}
