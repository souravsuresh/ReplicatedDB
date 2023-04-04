package com.wisc.raft.server;

import com.wisc.raft.constants.CommandType;
import com.wisc.raft.constants.Role;
import com.wisc.raft.dto.PeerInfo;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.RaftServiceGrpc;
import com.wisc.raft.state.NodeState;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private Map<String, ManagedChannel> channelMap = new HashMap<>(); // Ideally in a peer object


    private List<Raft.ServerConnect> cluster;

    //Threading
    private ExecutorService executorService; //Generic submitter
    private ScheduledExecutorService scheduledExecutorService;

    private ScheduledExecutorService schNodeEntries;

    private ScheduledFuture electionScheduler;
    private ScheduledFuture heartBeatScheduler;

    public Lock getLock() {
        return lock;
    }

    private Lock lock;


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
        lock = new ReentrantLock();
        //TODO : Loop - fill the map
        List<Integer> matchIndex = new ArrayList<>();
        List<Integer> nextIndex = new ArrayList<>();
        System.out.println("CLuster size : " + cluster.size());
        cluster.stream().forEach(serv -> {
            matchIndex.add(-1);
            nextIndex.add(0);
            if(serv.getServerId() != Integer.parseInt(this.state.getNodeId())){
                ManagedChannel channel = ManagedChannelBuilder.forAddress(serv.getEndpoint().getHost(), serv.getEndpoint().getPort())
                        .usePlaintext()
                        .build();
                channelMap.put(String.valueOf(serv.getServerId()), channel);
            }

        });

        this.state.setNextIndex(nextIndex);
        this.state.setMatchIndex(matchIndex);


        //Defining threading
        executorService = new ThreadPoolExecutor(10, 10, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        scheduledExecutorService = Executors.newScheduledThreadPool(4);
        schNodeEntries = Executors.newScheduledThreadPool(1);
        resetElectionTimeout();
    }


    private void resetElectionTimeout() throws InterruptedException {
        System.out.println("[ResetElectionTimeout] Inside reset election timeout!!");

        if (electionScheduler != null && !electionScheduler.isDone()) {
            electionScheduler.cancel(true);
        }

        lock.lock();
        try {
            if (this.state.getNodeType().equals(Role.LEADER)) {
                System.out.println("[ResetElectionTimeout]  Already a leader! So not participating in Election!");
                System.out.println(Thread.currentThread().getName());
                electionScheduler.cancel(true);
                System.out.println(Thread.currentThread().getName() + " HEllo World ");
                return;
            }
            this.state.setNodeType(Role.CANDIDATE); //Bit Risky
        } finally {
            lock.unlock();
        }

        electionTimeout = 100 + random.nextDouble() * ELECTION_TIMEOUT_INTERVAL;
        System.out.println("[ResetElectionTimeout] Inside resetElectionTimeout " + " : " + electionTimeout);


        electionScheduler = scheduledExecutorService.schedule(() -> {
            try {
                initiateRequestVoteRPC();
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("HELP ME" + e);
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
        Raft.RequestVote.Builder requestBuilder = Raft.RequestVote.newBuilder();

        lock.lock();
        try {
            if (!Objects.isNull(this.state.getVotedFor())) {
                return;
            }
            count = 0;
            this.state.setVotedFor(null);
            // increment current term
            System.out.println("[InitiateVote] Inside initiateRequestVoteRPC  !!");
            this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
            // voting for self
            this.state.setVotedFor(this.state.getNodeId());
            // @TODO : send Request vote RPC all servers (extend with threads to all server)
            requestBuilder.setCandidateId(this.state.getNodeId());
            requestBuilder.setTerm(this.state.getCurrentTerm());
            requestBuilder.setLastLogIndex(this.state.getCommitIndex());
            count++;
            long lastLogTerm = this.state.getCommitIndex() == 0 ? 0 : this.state.getEntries().get(this.state.getEntries().size() - 1).getTerm();
            requestBuilder.setLastLogTerm(lastLogTerm);
        } finally {
            lock.unlock();
        }
        Raft.RequestVote request = requestBuilder.build();

        // @TODO : call the server with above request object!1
        cluster.stream().forEach(serv -> {
            if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                executorService.submit(() -> requestVote(request, serv));
            }

        });

        resetElectionTimeout();

    }

    private void startHeartBeatAsLeader() throws InterruptedException {
        System.out.println("Inside startHeartBeatAsLeader");
        cluster.stream().forEach(serv -> {
            if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        sendAppendEntries(serv);
                    }
                });
            }
        });
        System.out.println("[startHeartBeatAsLeader] Waiting for all threads to complete");
//        System.out.println("Done with one heartBeat");
//        System.out.println("Match_Index : " + this.state.getMatchIndex());
//        System.out.println("Next_Index : " + this.state.getNextIndex());
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
        }, 40, TimeUnit.SECONDS);
    }


    //TODO PEER param
    private void requestVote(Raft.RequestVote request, Raft.ServerConnect server) {
        Raft.Endpoint endpoint = server.getEndpoint();
        ManagedChannel channel = channelMap.get(String.valueOf(server.getServerId()));
        System.out.println("[RequestVoteWrapper] Inside requestVote for endpoint :: " + endpoint.getPort());



        System.out.println("[RequestVoteWrapper] Channel state :: " + channel.getState(true) + " :: " + channel.getState(false));
        if (channel.getState(false) == ConnectivityState.TRANSIENT_FAILURE) {
            System.out.println("Follower is down!! " + endpoint.getPort());
            return;
        }
        System.out.println("[RequestVoteWrapper] CandidateID : " + request.getCandidateId());
        RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channelMap.get(channel));

        Raft.ResponseVote responseVote = raftServiceBlockingStub.requestVotes(request);

        if (responseVote.getGrant()) {
            //What if he became leader
            lock.lock();
            try {
//                if (this.state.getCurrentTerm() != responseVote.getTerm() || this.state.getNodeType() != Role.CANDIDATE) {
//                    System.out.println("[RequestVoteWrapper] The response term is mismatched or Current node state is not candidate :: " + this.state.getNodeType());
//
//                }

                if (responseVote.getTerm() > this.state.getCurrentTerm()) {
                    System.out.println("[RequestVoteWrapper] Received term response from other follower :: " + responseVote.getTerm() + " more than current term ::" + this.state.getCurrentTerm());
                    this.state.setNodeType(Role.FOLLOWER);
                    this.state.setCurrentTerm(responseVote.getTerm());
                    if (heartBeatScheduler != null && !heartBeatScheduler.isDone()) {
                        heartBeatScheduler.cancel(true);
                    }
                    resetElectionTimeout();
                } else {
                    System.out.println("[InitiateVote] Before voting : " + count);
                    count++;

                    lock.lock();
                    try {
                        
                        if (this.state.getNodeType() != Role.CANDIDATE) {
                            System.out.println("[InitiateVote] Some other guy took over the leadership!! ");
                        } else if (count > cluster.size() / 2) {
                            System.out.println("[InitiateVote] Got the leadership ::  NodeType : " + this.state.getNodeType() + " Votes : " + count + " current term: " + this.state.getCurrentTerm());
                            this.state.setNodeType(Role.LEADER);
                            if (electionScheduler != null && !electionScheduler.isDone()) {
                                electionScheduler.cancel(true);
                            }
                            startHeartBeatAsLeader();
                        }
                        System.out.println("[InitiateVote] Number of Votes : " + count);

                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        lock.unlock();
                    }

                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        } else {
            System.out.println("[RequestVoteWrapper] Not granted by :: " + endpoint.getPort() + " Response :: " + responseVote.getTerm() + " Current :: " + this.state.getCurrentTerm());
        }

    }

    // @TODO send as Actual RPC response (write a wrapper for this!!)

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
        List<Raft.LogEntry> entryToSend = new ArrayList<>();

        Raft.AppendEntriesRequest.Builder requestBuilder = Raft.AppendEntriesRequest.newBuilder();

        lock.lock();
        try {
            System.out.println("sendAppendEntries - 1 " + server.getServerId());
            if (this.state.getNodeType() != Role.LEADER) {
                System.out.println("sendAppendEntries : I can't do this");
                return;
            }
            int followerIndex = this.state.getNextIndex().get(server.getServerId());
            long peerMatchIndex = this.state.getMatchIndex().get(server.getServerId()); //-1
            long currentIndex = this.state.getCommitIndex();
            long currentTerm = this.state.getCurrentTerm();
            String nodeId = this.state.getNodeId();
            if (currentIndex < followerIndex) {
                System.out.println("This cannot happen right ??");
                return;
            }

            if (followerIndex == 0 || this.state.getEntries().isEmpty()) {
                requestBuilder.setPrevLogTerm(0);
                requestBuilder.setPrevLogIndex(-1);
            } else {
                requestBuilder.setPrevLogTerm(this.state.getEntries().get(followerIndex - 1).getTerm());
                requestBuilder.setPrevLogIndex(followerIndex - 1);

            }

            requestBuilder.setTerm(currentTerm); // Is this correct ?
            requestBuilder.setLeaderId(nodeId);

            requestBuilder.setCommitIndex(this.state.getCommitIndex()); // Last Commit Index
            List<Raft.LogEntry> entries = this.state.getEntries();

            //ConvertToInt
            for (long i = peerMatchIndex + 1; i < entries.size(); i++) {
                entryToSend.add(entries.get((int) i));
            }
            requestBuilder.addAllEntries(entryToSend);
            System.out.println("sendAppendEntries : I " + server.getServerId());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("HB");
        } finally {
            lock.unlock();

        }

        // @CHECK :: Snapshot might mess this up!! Possibility is this.state.term
        //Check last match Index and send everything from there to current irrespective if it is HB
        System.out.println("sendAppendEntries : before call : " + server.getServerId());

        ManagedChannel channel = channelMap.get(String.valueOf(server.getServerId()));
        RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);

        Raft.AppendEntriesResponse response = raftServiceBlockingStub.appendEntries(requestBuilder.build());

        //TODO check for exception
        lock.lock();
        try {
            boolean success = response.getSuccess();
            if(this.state.getCurrentTerm() < response.getTerm()){
                this.state.setNodeType(Role.FOLLOWER);
                // stop heartbeat
                if (heartBeatScheduler != null && !heartBeatScheduler.isDone()) {
                    heartBeatScheduler.cancel(true);
                }
                resetElectionTimeout();
            }
            if (!success) {
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
            } else {
                System.out.println("Post Response");

                if (entryToSend.size() == 0) {
                    return;
                }

                    this.state.getNextIndex().set(server.getServerId(), (int) response.getLastMatchIndex() + 1);
                    this.state.getMatchIndex().set(server.getServerId(), (int) response.getLastMatchIndex());


                return;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
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

    public void downGrade(Role role) throws InterruptedException {


        this.getState().setNodeType(role);
        this.state.setVotedFor(null);

        if (heartBeatScheduler != null && !heartBeatScheduler.isDone()) {
            heartBeatScheduler.cancel(true);
        }
        resetElectionTimeout();
//        if (electionScheduler != null && !electionScheduler.isDone()) {
//            electionScheduler.cancel(true);
//        }

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

    public void cancelHB(){
        if (heartBeatScheduler != null && !heartBeatScheduler.isDone()) {
            heartBeatScheduler.cancel(true);
        }
    }
}
