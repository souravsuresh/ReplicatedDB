package com.wisc.raft.server;

import com.wisc.raft.constants.Role;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.RaftServiceGrpc;
import com.wisc.raft.service.Database;
import com.wisc.raft.service.RaftConsensusService;
import com.wisc.raft.state.NodeState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Getter
@Setter
public class Server {
    private static final Random random = new Random();
    private static final long ELECTION_TIMEOUT_INTERVAL = 100;  //100ms
    private static final long HEARTBEAT_TIMEOUT_INTERVAL = 80;  //80ms
    private static final long MAX_REQUEST_RETRY = 3;  // retry requests for 3 times

    private static long logAppendRetries = 0;
    // @TODO: for now maintaining HashMap!! Update this to db updates
    private HashMap<Long, Long> kvStore = new HashMap<>();
    private NodeState state;
    private List<Raft.ServerConnect> cluster;

    //Threading
    private ScheduledExecutorService electionExecutorService;
    private ScheduledExecutorService heartbeatExecutorService;
    private ScheduledExecutorService commitSchedulerService;


    private ScheduledFuture electionScheduler;
    private ScheduledFuture heartBeatScheduler;

    private ScheduledFuture commitScheduler;

    private ThreadPoolExecutor electionExecutor;
    private ThreadPoolExecutor heartBeatExecutor;

    private ThreadPoolExecutor commitExecutor;


    private ExecutorService appendEntriesExecutor;
    public Lock getLock() {
        return lock;
    }

    public void setLock(Lock lock) {
        this.lock = lock;
    }

    private Lock lock;

    private Database db;

    private Map<String, Boolean> persistentStore;

    private RaftConsensusService raftConsensusService;

    public Server(String nodeId) {
        this.state = new NodeState(nodeId);
    }

    public void init() {
        lock = new ReentrantLock();
        //db = new Database();
        List<Integer> matchIndex = new ArrayList<>();
        List<Integer> nextIndex = new ArrayList<>();
        System.out.println("Cluster size : " + cluster.size());
        cluster.stream().forEach(serv -> {
            matchIndex.add(-1);
            nextIndex.add(0);
        });
        this.state.setNextIndex(nextIndex);
        this.state.setMatchIndex(matchIndex);
        //TODO : Change these details via configuration
        Runnable initiateElectionRPCRunnable = () -> initiateElectionRPC();
        Runnable initiateHeartbeatRPCRunnable = () -> initiateHeartbeatRPC();
        Runnable initiateElectionExecutorRunnable = () -> initiateCommitScheduleRPC();
        this.electionExecutor = new ThreadPoolExecutor(cluster.size(), cluster.size(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        this.heartBeatExecutor = new ThreadPoolExecutor(cluster.size(), cluster.size(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        electionExecutorService = Executors.newSingleThreadScheduledExecutor();
        commitSchedulerService = Executors.newSingleThreadScheduledExecutor();
        commitScheduler = commitSchedulerService.scheduleAtFixedRate(initiateElectionExecutorRunnable, 1, 10, TimeUnit.SECONDS);
        electionScheduler = electionExecutorService.scheduleAtFixedRate(initiateElectionRPCRunnable, 1, 5, TimeUnit.SECONDS);
        // electionScheduler = electionExecutorService.scheduleAtFixedRate(initiateElectionRPCRunnable, 1L, (long) (100 + random.nextDouble() * ELECTION_TIMEOUT_INTERVAL), TimeUnit.SECONDS);
        heartbeatExecutorService = Executors.newSingleThreadScheduledExecutor();
        heartBeatScheduler = heartbeatExecutorService.scheduleAtFixedRate(initiateHeartbeatRPCRunnable, 5, 2, TimeUnit.SECONDS);
    }

    private void initiateCommitScheduleRPC(){
        lock.lock();
        try {
            if (this.state.getNodeType() == Role.LEADER) {
                if(this.state.getCommitIndex() <= this.state.getLastApplied() &&  this.state.getCommitIndex() <= this.state.getEntries().size() && this.state.getLastApplied() <=  this.state.getEntries().size()){
                    long index = this.state.getCommitIndex();
                    System.out.println("[CommitSchedule] inside leader commit starting from " + index + 1 + " to "+ this.state.getLastApplied());
                    for(long i = index+1; i<=this.state.getLastApplied(); i++){
                        int ret = db.commit(this.state.getEntries().get((int) i));
                        if(ret == -1){
                            System.out.println("[CommitSchedule] Failed but no issues");
                        }
                        else{
                            System.out.println("[CommitSchedule] Commited successfully :: "+i);
                        }
                        this.persistentStore.put(this.state.getEntries().get((int) i).getRequestId(), true);
                        this.state.setCommitIndex(this.state.getCommitIndex() + 1);
                    }
                }
            }
            else{
                if(this.state.getCommitIndex() > this.state.getLastLeaderCommitIndex()){
                    System.out.println("[CommitSchedule] Your commit index :: " + this.state.getCommitIndex() + " is more than leader commit index :: "+this.state.getLastLeaderCommitIndex());
                    return;
                }
                if(this.state.getCommitIndex() <= this.state.getLastLeaderCommitIndex()  && this.state.getCommitIndex() < this.state.getEntries().size() && this.state.getLastLeaderCommitIndex() < this.state.getEntries().size()){
                    long index = this.state.getCommitIndex();
                    for(long i=index+1;i<=this.state.getLastLeaderCommitIndex();i++){
                        int ret = db.commit(this.state.getEntries().get((int) i));
                        if(ret == -1){
                            System.out.println("[CommitSchedule] Failed but no issues");
                        }
                        else{
                            System.out.println("[CommitSchedule] Commited successfully :: "+i);
                        }
                        this.persistentStore.put(this.state.getEntries().get((int) i).getRequestId(), true);
                        this.state.setCommitIndex(this.state.getCommitIndex() + 1);
                    }
                }
            }
        } catch (Exception ex) {
            System.out.println("[CommitSchedule] Oops got a intresting exception:: "+ex);
        }
        finally {
            lock.unlock();
        }
    }
    public void initiateElectionRPC() {
        Raft.RequestVote.Builder requestBuilder = Raft.RequestVote.newBuilder();
        System.out.println("Starting election at :: "+ System.currentTimeMillis());
        lock.lock();
        try {
            System.out.println("[initiateElectionRPC] Current time :: " + System.currentTimeMillis() + " HeartBeat timeout time :: " +  (this.state.getHeartbeatTrackerTime() + 5 * 1000 * MAX_REQUEST_RETRY));
            if(this.state.getHeartbeatTrackerTime() != 0 && System.currentTimeMillis() > (this.state.getHeartbeatTrackerTime() +  5 * 1000 * MAX_REQUEST_RETRY) ) {
                System.out.println("[initiateElectionRPC] Stepping down as follower");
                this.state.setVotedFor(null);
                this.state.setNodeType(Role.FOLLOWER);
            }
            if (this.state.getNodeType().equals(Role.LEADER)) {
                System.out.println("[initiateElectionRPC]  Already a leader! So not participating in Election!");
                //@TODO :: remove this once client code is up
                for(int i=0;i<1;i++){
                    this.state.getSnapshot().add(Raft.LogEntry.newBuilder().setCommand(Raft.Command.newBuilder().setValue(random.nextInt(10)).setKey(random.nextInt(10)).build()).setTerm(this.state.getCurrentTerm()).setIndex("Bolimaga").build());
                }
                return;
            }
            if (!Objects.isNull(this.state.getVotedFor()) && this.state.getNodeType().equals(Role.FOLLOWER)) {
                System.out.println("[initiateElectionRPC]  Already voted ! So not participating in Election! : " + this.state.getVotedFor());
                return;
            }

            System.out.println("[initiateElectionRPC] Starting the voting process");
            this.state.setVotedFor(this.state.getNodeId());
            this.state.setNodeType(Role.CANDIDATE);
            this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
            requestBuilder.setCandidateId(this.state.getNodeId());
            requestBuilder.setTerm(this.state.getCurrentTerm());
            long lastLogTerm = this.state.getLastApplied() == 0 ? -1 : this.state.getEntries().get((int)this.state.getLastApplied()).getTerm();
            requestBuilder.setLeaderLastAppliedTerm(lastLogTerm);
            requestBuilder.setLeaderLastAppliedIndex(this.state.getLastApplied());
            this.state.setTotalVotes(1);
        }
        catch(Exception e){
            System.out.println(e);
        }
        finally {
            lock.unlock();
        }
        Raft.RequestVote request = requestBuilder.build();
        cluster.stream().forEach(serv -> {
            if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                this.electionExecutor.submit(() -> requestVote(request, serv));
            }
        });
    }

    private void requestVote(Raft.RequestVote request, Raft.ServerConnect server) {
        Raft.Endpoint endpoint = server.getEndpoint();
        System.out.println("[RequestVoteWrapper] Inside requestVote for endpoint :: " + endpoint.getPort());

        ManagedChannel channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build();
        System.out.println("[RequestVoteWrapper] Channel state :: " + channel.getState(true) + " :: " + channel.getState(false));
        System.out.println("[RequestVoteWrapper] Sent voting req : " + request.getCandidateId());
        try {
            RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);
            Raft.ResponseVote responseVote = raftServiceBlockingStub.requestVotes(request);
            if (responseVote.getGrant()) {
                lock.lock();
                try {
                    this.state.setTotalVotes(this.state.getTotalVotes() + 1);
                    if (this.state.getNodeType() != Role.CANDIDATE) {
                        System.out.println("[RequestVoteWrapper] Some other guy took over the leadership!! ");
                    } else if (this.state.getTotalVotes() > cluster.size() / 2) {
                        System.out.println("[RequestVoteWrapper] Got the leadership ::  NodeType : " + this.state.getNodeType() + " Votes : " + this.state.getTotalVotes() + " current term: " + this.state.getCurrentTerm());
                        this.state.setHeartbeatTrackerTime(System.currentTimeMillis());
                        this.state.setNodeType(Role.LEADER);
                    }
                    System.out.println("[RequestVoteWrapper] Number of Votes : " + this.state.getTotalVotes());
                } finally {
                    lock.unlock();
                }
            } else {
                System.out.println("[RequestVoteWrapper] Not granted by :: " + endpoint.getPort() + " Response :: " + responseVote.getTerm() + " Current :: " + this.state.getCurrentTerm());
            }
        }catch (Exception ex) {
                System.out.println("[RequestVoteWrapper] Server might not be up!! "+ ex);
        }


    }

    public void initiateHeartbeatRPC() {

        try {
            System.out.println("[RaftService] Log Entries has " + this.state.getEntries().size() + " entries");
            if (!this.state.getNodeType().equals(Role.LEADER)) {
                System.out.println("[initiateHeartbeatRPC] Not a leader! So not participating in HeartBeat!");
                return;
            }

            if (this.state.getLastApplied() < this.state.getLastLogIndex()) {
                // check for majority now!
                int i = 0, majority = 1;
                for(; i < cluster.size();++i) {
                    int serverInd = cluster.get(i).getServerId();
                    if(serverInd != Integer.parseInt(this.state.getNodeId())
                            && this.state.getMatchIndex().get(serverInd) == this.state.getLastLogIndex()) {
                        majority += 1;
                    }
                    System.out.println("Server index :: "+ serverInd + " has match index :: "+this.state.getMatchIndex());
                }
                if (majority <= cluster.size()/2) {
                    if(logAppendRetries == MAX_REQUEST_RETRY) {
                        System.out.println("[initiateHeartbeatRPC] Log append retries max limit reached!!");
                        this.state.getEntries().subList((int) this.state.getLastApplied(), (int) this.state.getLastLogIndex() + 1).clear();
                        this.state.setLastLogIndex(this.state.getEntries().size() - 1);
                    } else {
                        System.out.println("[initiateHeartbeatRPC] Retrying the log append retires!!");
                        logAppendRetries++;
                    }
                } else {
                    logAppendRetries = 0;
                }
                if(logAppendRetries == 0 || logAppendRetries == MAX_REQUEST_RETRY) {
                    System.out.println("Max retry reached!!");

                    this.state.setLastApplied(this.state.getLastLogIndex());
                    // apply snapshot
                    this.state.getEntries().addAll(this.state.getSnapshot());
                    int size = (int) (this.state.getLastLogIndex() + this.state.getSnapshot().size());
//                    if(this.state.getLastLogIndex() == 0){
//                        if(this.state.getSnapshot().size() > 0) size -= 1;
//                    }
                    this.state.setLastLogIndex(size);
                    logAppendRetries = 0;
                    this.state.getSnapshot().clear();
                }
            } else {
                this.state.getEntries().addAll(this.state.getSnapshot());
                int size = (int) (this.state.getLastLogIndex() + this.state.getSnapshot().size());
//                if(this.state.getLastLogIndex() == 0){
//                    if(this.state.getSnapshot().size() > 0) size -= 1;
//                }
                this.state.setLastLogIndex(size);
                this.state.getSnapshot().clear();
            }
            System.out.println("[initiateHeartbeatRPC] Snapshot :: "+ this.state.getSnapshot());
            System.out.println("[initiateHeartbeatRPC] Entries :: "+ this.state.getEntries());
            System.out.println("[initiateHeartbeatRPC] Current Node is a leader! Reseting the heartbeat timeout to :: "+System.currentTimeMillis());
            this.state.setHeartbeatTrackerTime(System.currentTimeMillis());

            this.state.getEntries().stream().forEach(le ->
                    System.out.println(le.getTerm() + " :: " + le.getCommand().getKey() +" -> "+le.getCommand().getValue()));

            cluster.stream().forEach(serv -> {
                if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                    this.heartBeatExecutor.submit(() -> sendAppendEntries(serv));
                }
            });
        } catch (Exception e) {
            System.out.println("[initiateHeartbeatRPC] excp :: "+ e);
        }

    }

    private void sendAppendEntries(Raft.ServerConnect server) {

        System.out.println("[sendAppendEntries] : Sending request to " + server.getServerId() + " at "+System.currentTimeMillis());
        List<Raft.LogEntry> entryToSend = new ArrayList<>();

        Raft.AppendEntriesRequest.Builder requestBuilder = Raft.AppendEntriesRequest.newBuilder();
        try {
            if (this.state.getNodeType() != Role.LEADER) {
                System.out.println("[sendAppendEntries] : Current node is not leader so cant send heartbeats");
                return;
            }
            int followerIndex = this.state.getNextIndex().get(server.getServerId());
            long peerMatchIndex = this.state.getMatchIndex().get(server.getServerId()); //-1
            long currentIndex = this.state.getCommitIndex();
            long currentTerm = this.state.getCurrentTerm();
            String nodeId = this.state.getNodeId();

            if (followerIndex == 0 || this.state.getEntries().isEmpty()) {
                requestBuilder.setPrevLogIndex(0);
                requestBuilder.setPrevLogTerm(-1);
            } else {
                requestBuilder.setPrevLogTerm(this.state.getEntries().get((int) this.state.getLastApplied()).getTerm());
                requestBuilder.setPrevLogIndex(this.state.getLastApplied());
            }

            requestBuilder.setTerm(currentTerm); // Is this correct ?
            requestBuilder.setLeaderId(nodeId);

            requestBuilder.setCommitIndex(this.state.getCommitIndex()); // Last Commit Index
            List<Raft.LogEntry> entries = this.state.getEntries();
            System.out.println("[sendAppendEntries] peer match :: " + peerMatchIndex + " : "+this.state.getLastLogIndex());
            System.out.println("[sendAppendEntries] Snapshot :: "+ this.state.getSnapshot());
            System.out.println("[sendAppendEntries] Entries :: "+ this.state.getEntries());
            //ConvertToInt
            for (long i = peerMatchIndex + 1; i <= this.state.getLastLogIndex(); i++) {
                entryToSend.add(entries.get((int) i));
            }
            requestBuilder.addAllEntries(entryToSend);
            System.out.println("[sendAppendEntries] Final Request :: "+ requestBuilder.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[sendAppendEntries] ex : "+ e);
        }

        // @CHECK :: Snapshot might mess this up!! Possibility is this.state.term
        //Check last match Index and send everything from there to current irrespective if it is HB
        System.out.println("[sendAppendEntries] : before call : " + server.getServerId());
        Raft.Endpoint endpoint = server.getEndpoint();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build();
        RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);
        Raft.AppendEntriesResponse response = raftServiceBlockingStub.appendEntries(requestBuilder.build());

        lock.lock();
        try {
            boolean success = response.getSuccess();
            if(this.state.getCurrentTerm() < response.getTerm()){
                this.state.setNodeType(Role.FOLLOWER);
                System.out.println("Got rejected, as my term was lower as a leader. This shouldn't be happening");
            }
            if (!success) {
                long term = response.getTerm();

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
                System.out.println("[sendAppendEntries] Post Response");
                this.state.getNextIndex().set(server.getServerId(), (int) response.getLastMatchIndex() + 1);
                this.state.getMatchIndex().set(server.getServerId(), (int) response.getLastMatchIndex());
            }
        } catch (Exception e) {
            System.out.println("[sendAppendEntries] after response ex : " + e);
        } finally {
            lock.unlock();
        }
    }

    public long getValue(long key) {
        long value = db.read(key);
        return value;
    }


    public int putValue(long key, long val) {
        lock.lock();
        try {
            int numOfEntries = 1;
            //TODO should we pull the leader check code there ?
            //TODO add cmd type from params and make this into a loop
            Raft.Command command = Raft.Command.newBuilder().setCommandType("Something").setKey(key).setValue(val).build();
            String requestId = String.valueOf(UUID.randomUUID());
            Raft.LogEntry logEntry = Raft.LogEntry.newBuilder().setRequestId(requestId)
                                            .setCommand(command)
                                                .setIndex(String.valueOf(this.state.getEntries().size()))
                                                .setTerm(this.state.getCurrentTerm()).build();

            this.persistentStore.put(requestId, false);
            this.state.getSnapshot().add(logEntry);
            System.out.println("Created request with id :: "+ requestId);
            return 0;
        } finally {
            lock.unlock();
        }

    }

}
