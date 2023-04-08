package com.wisc.raft.server;

import com.wisc.raft.RaftServer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Getter
@Setter
public class Server {
    private static final Logger logger =  LoggerFactory.getLogger(RaftServer.class);

    private static final Random random = new Random();
    private static final long ELECTION_TIMEOUT_INTERVAL = 100;  //100ms
    private static final long HEARTBEAT_TIMEOUT_INTERVAL = 80;  //80ms
    private static final long MAX_REQUEST_RETRY = 3;  // retry requests for 3 times

    private static long termIncreaseInterval = 3;  // retry requests for 3 times


    private static long logAppendRetries = 0;
    private static long rejectionRetries = 0;
    // @TODO: for now maintaining HashMap!! Update this to db updates
    private HashMap<Long, Long> kvStore = new HashMap<>();
    private NodeState state;
    private List<Raft.ServerConnect> cluster;

    //Threading
    private ScheduledExecutorService electionExecutorService;
    private ScheduledExecutorService heartbeatExecutorService;
    private ScheduledExecutorService commitSchedulerService;

    private ScheduledExecutorService replySchedulerService;



    private ScheduledFuture electionScheduler;
    private ScheduledFuture heartBeatScheduler;

    private ScheduledFuture commitScheduler;

    private ScheduledFuture replyScheduler;


    private ThreadPoolExecutor electionExecutor;
    private ThreadPoolExecutor heartBeatExecutor;

    private ThreadPoolExecutor commitExecutor;

    private ThreadPoolExecutor replyExecutor;





    private ExecutorService appendEntriesExecutor;
    public Lock getLock() {
        return lock;
    }

    public void setLock(Lock lock) {
        this.lock = lock;
    }

    private Lock lock;

    private Database db;

    private ManagedChannel clientChannel;

    private ServerClientConnectionGrpc.ServerClientConnectionBlockingStub serverClientConnectionBlockingStub;
    private ConcurrentHashMap<String, Boolean> persistentStore;

    private RaftConsensusService raftConsensusService;

    public Server(String nodeId, Database db) {
        this.state = new NodeState(nodeId);
        this.db = db;
    }

    public void init(String clientHost, int port) {
        lock = new ReentrantLock();
        //db = new Database();
        List<Integer> matchIndex = new ArrayList<>();
        List<Integer> nextIndex = new ArrayList<>();
        logger.info("Cluster size : " + cluster.size());
        cluster.stream().forEach(serv -> {
            matchIndex.add(-1);
            nextIndex.add(0);
        });
        clientChannel = ManagedChannelBuilder.forAddress(clientHost, port).usePlaintext().build();
        serverClientConnectionBlockingStub = ServerClientConnectionGrpc.newBlockingStub(clientChannel);

        persistentStore = new ConcurrentHashMap<>();
        this.state.setNextIndex(nextIndex);
        this.state.setMatchIndex(matchIndex);
        //TODO : Change these details via configuration
        Runnable initiateElectionRPCRunnable = () -> initiateElectionRPC();
        Runnable initiateHeartbeatRPCRunnable = () -> initiateHeartbeatRPC();
        Runnable initiateElectionExecutorRunnable = () -> initiateCommitScheduleRPC();
        Runnable replyClientExecutorRunnable = () -> initiateReplyScheduleRPC();
        this.electionExecutor = new ThreadPoolExecutor(cluster.size(), cluster.size(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        this.heartBeatExecutor = new ThreadPoolExecutor(cluster.size(), cluster.size(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        electionExecutorService = Executors.newSingleThreadScheduledExecutor();
        commitSchedulerService = Executors.newSingleThreadScheduledExecutor();
        replySchedulerService = Executors.newSingleThreadScheduledExecutor();
        replyScheduler = replySchedulerService.scheduleAtFixedRate(replyClientExecutorRunnable,20, 30, TimeUnit.SECONDS);
        commitScheduler = commitSchedulerService.scheduleAtFixedRate(initiateElectionExecutorRunnable, 1, 10, TimeUnit.SECONDS);
        electionScheduler = electionExecutorService.scheduleAtFixedRate(initiateElectionRPCRunnable, 1, 5, TimeUnit.SECONDS);
        // electionScheduler = electionExecutorService.scheduleAtFixedRate(initiateElectionRPCRunnable, 1L, (long) (100 + random.nextDouble() * ELECTION_TIMEOUT_INTERVAL), TimeUnit.SECONDS);
        heartbeatExecutorService = Executors.newSingleThreadScheduledExecutor();
        heartBeatScheduler = heartbeatExecutorService.scheduleAtFixedRate(initiateHeartbeatRPCRunnable, 5, 2, TimeUnit.SECONDS);
    }

    private void initiateReplyScheduleRPC(){
        //Get entries which are true;

        for (String reqId : persistentStore.keySet()) {
            if(persistentStore.get(reqId) != null){
                this.serverClientConnectionBlockingStub.talkBack(Client.StatusUpdate.newBuilder().setReqId(reqId).setReturnVal(persistentStore.get(reqId)).build());
                persistentStore.remove(reqId);
            }

        }
    }
    private void initiateCommitScheduleRPC(){
        lock.lock();
        try {
            if (this.state.getNodeType() == Role.LEADER) {
                if(this.state.getCommitIndex() <= this.state.getLastApplied() &&  this.state.getCommitIndex() <= this.state.getEntries().size() && this.state.getLastApplied() <=  this.state.getEntries().size()){
                    long index = this.state.getCommitIndex();
                    logger.debug("[CommitSchedule] inside leader commit starting from " + index + 1 + " to "+ this.state.getLastApplied());
                    for(long i = index+1; i<=this.state.getLastApplied(); i++){
                        int ret = db.commit(this.state.getEntries().get((int) i));
                        if(ret == -1){
                            logger.warn("[CommitSchedule] Failed but no issues");
                        }
                        else{
                            logger.debug("[CommitSchedule] Commited successfully :: "+i);
                        }
                        this.persistentStore.put(this.state.getEntries().get((int) i).getRequestId(), true);
                        this.state.setCommitIndex(this.state.getCommitIndex() + 1);
                    }
                }
            }
            else{
                if(this.state.getCommitIndex() > this.state.getLastLeaderCommitIndex()){
                    logger.warn("[CommitSchedule] Your commit index :: " + this.state.getCommitIndex() + " is more than leader commit index :: "+this.state.getLastLeaderCommitIndex());
                    return;
                }
                if(this.state.getCommitIndex() <= this.state.getLastLeaderCommitIndex()  && this.state.getCommitIndex() < this.state.getEntries().size() && this.state.getLastLeaderCommitIndex() < this.state.getEntries().size()){
                    long index = this.state.getCommitIndex();
                    for(long i=index+1;i<=this.state.getLastLeaderCommitIndex();i++){
                        int ret = db.commit(this.state.getEntries().get((int) i));
                        if(ret == -1){
                            logger.warn("[CommitSchedule] Failed but no issues");
                        }
                        else{
                            logger.debug("[CommitSchedule] Commited successfully :: "+i);
                        }
                        this.persistentStore.put(this.state.getEntries().get((int) i).getRequestId(), ret != -1);
                        this.state.setCommitIndex(this.state.getCommitIndex() + 1);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("[CommitSchedule] Oops got a intresting exception:: "+ex);
        }
        finally {
            lock.unlock();
        }
    }
    public void initiateElectionRPC() {
        Raft.RequestVote.Builder requestBuilder = Raft.RequestVote.newBuilder();
        logger.debug("Starting election at :: "+ System.currentTimeMillis());
        lock.lock();
        try {
            logger.debug("[initiateElectionRPC] Current time :: " + System.currentTimeMillis() + " HeartBeat timeout time :: " +  (this.state.getHeartbeatTrackerTime() + 5 * 1000 * MAX_REQUEST_RETRY));
            if(this.state.getHeartbeatTrackerTime() != 0 && System.currentTimeMillis() > (this.state.getHeartbeatTrackerTime() +  5 * 1000 * MAX_REQUEST_RETRY) ) {
                logger.debug("[initiateElectionRPC] Stepping down as follower");
                this.state.setVotedFor(null);
                this.state.setNodeType(Role.FOLLOWER);
            }
            if (this.state.getNodeType().equals(Role.LEADER)) {
                logger.debug("[initiateElectionRPC]  Already a leader! So not participating in Election!");
                //@TODO :: remove this once client code is up
//                for(int i=0;i<1;i++){
//                    this.state.getSnapshot().add(Raft.LogEntry.newBuilder().setCommand(Raft.Command.newBuilder().setValue(random.nextInt(10)).setKey(random.nextInt(10)).build()).setTerm(this.state.getCurrentTerm()).setIndex("Bolimaga").build());
//                }
                return;
            }
            if (!Objects.isNull(this.state.getVotedFor()) && this.state.getNodeType().equals(Role.FOLLOWER)) {
                logger.debug("[initiateElectionRPC]  Already voted ! So not participating in Election! : " + this.state.getVotedFor());
                return;
            }

            logger.debug("[initiateElectionRPC] Starting the voting process");
            this.state.setVotedFor(this.state.getNodeId());
            this.state.setNodeType(Role.CANDIDATE);
            this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
            requestBuilder.setCandidateId(this.state.getNodeId());
            requestBuilder.setTerm(this.state.getCurrentTerm());
            long lastLogTerm = this.state.getLastApplied() == -1 ? -1 : this.state.getEntries().get((int)this.state.getLastApplied()).getTerm();
            requestBuilder.setLeaderLastAppliedTerm(lastLogTerm);
            requestBuilder.setLeaderLastAppliedIndex(this.state.getLastApplied());
            this.state.setTotalVotes(1);
        }
        catch(Exception e){
            logger.error("Initiate RPC before request error " + e);
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
        logger.error("[RequestVoteWrapper] Inside requestVote for endpoint :: " + endpoint.getPort());

        ManagedChannel channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build();
        try {

            logger.error("[RequestVoteWrapper] Channel state :: " + channel.getState(true) + " :: " + channel.getState(false));
            logger.error("[RequestVoteWrapper] Sent voting req : " + request.getCandidateId());
            RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);
            Raft.ResponseVote responseVote = raftServiceBlockingStub.requestVotes(request);
            if (responseVote.getGrant()) {
                lock.lock();
                try {
                    this.state.setTotalVotes(this.state.getTotalVotes() + 1);
                    if (this.state.getNodeType() != Role.CANDIDATE) {
                        logger.warn("[RequestVoteWrapper] Some other guy took over the leadership!! ");
                    } else if (this.state.getTotalVotes() > cluster.size() / 2) {
                        logger.debug("[RequestVoteWrapper] Got the leadership ::  NodeType : " + this.state.getNodeType() + " Votes : " + this.state.getTotalVotes() + " current term: " + this.state.getCurrentTerm());
                        this.state.setHeartbeatTrackerTime(System.currentTimeMillis());
                        this.state.setNodeType(Role.LEADER);
                    }
                    logger.debug("[RequestVoteWrapper] Before state of "+server.getEndpoint()+" Match index :: "+ this.state.getMatchIndex().get(server.getServerId()) + " Next Index :: "+ this.state.getNextIndex().get(server.getServerId()));
                    this.state.getNextIndex().set(server.getServerId(), (int) responseVote.getCandidateLastLogIndex());
                    this.state.getMatchIndex().set(server.getServerId(), (int) responseVote.getCandidateLastAppliedLogIndex());
                    logger.debug("[RequestVoteWrapper] After state of "+server.getEndpoint()+" Match index :: "+ this.state.getMatchIndex().get(server.getServerId()) + " Next Index :: "+ this.state.getNextIndex().get(server.getServerId()));
                    logger.debug("[RequestVoteWrapper] Number of Votes : " + this.state.getTotalVotes());
                } finally {
                    lock.unlock();
                }
            } else {
                logger.debug("[RequestVoteWrapper] Not granted by :: " + endpoint.getPort() + " Response :: " + responseVote.getTerm() + " Current :: " + this.state.getCurrentTerm());
            }
        }catch (Exception ex) {
            logger.error("[RequestVoteWrapper] Server might not be up!! "+ ex);
        } finally{
            channel.shutdown();
        }


    }

    public void initiateHeartbeatRPC() {

        try {
            logger.debug("[RaftService] Log Entries has " + this.state.getEntries().size() + " entries" + " Jelp: " + logAppendRetries);
            if (!this.state.getNodeType().equals(Role.LEADER)) {
                logger.debug("[initiateHeartbeatRPC] Not a leader! So not participating in HeartBeat!");
                return;
            }

            termIncreaseInterval++;
            lock.lock();
            try{
                if (termIncreaseInterval > cluster.size() * 2) {

                    this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);

                    termIncreaseInterval = 0;
                }
            }
            finally{
                lock.unlock();
            }

            logger.debug(this.state.getLastApplied() + " getLastApplied :  " +  this.state.getLastLogIndex() + " : getLastLogIndex");
            if (this.state.getLastApplied() < this.state.getLastLogIndex()) {
                // check for majority now!
                int i = 0, majority = 1;
                for(; i < cluster.size();++i) {
                    int serverInd = cluster.get(i).getServerId();
                    if(serverInd != Integer.parseInt(this.state.getNodeId())
                            && this.state.getMatchIndex().get(serverInd) == this.state.getLastLogIndex()) {
                        majority += 1;
                    }
                    logger.debug("Server index :: "+ serverInd + " has match index :: "+this.state.getMatchIndex());
                }
                if (majority <= cluster.size()/2) {
                    logger.debug("[initiateHeartbeatRPC] Retrying the log append retires!! : " + logAppendRetries);
                    logAppendRetries++;
                } else {
                    logger.debug("[initiateHeartbeatRPC] Resetting everything : " + logAppendRetries);
                    rejectionRetries = 0;
                    logAppendRetries = 0;
                }
                if(logAppendRetries == 0){
                    logger.warn("Max retry reached!!");

                    this.state.setLastApplied(this.state.getLastLogIndex());
                    // apply snapshot
                    this.state.getEntries().addAll(this.state.getSnapshot());
                    this.state.setLastLogIndex(this.state.getEntries().size() - 1);
                    rejectionRetries = 0;
                    logAppendRetries = 0;
                    this.state.getSnapshot().clear();
                }
                if(logAppendRetries == MAX_REQUEST_RETRY) {
                    if(rejectionRetries == MAX_REQUEST_RETRY) {
                        logger.warn("[initiateHeartbeatRPC] Max rejections seen from the leader! Stepping down!");
                        logAppendRetries = 0;
                        rejectionRetries = 0;
                        this.state.setNodeType(Role.FOLLOWER);
                        return;
                    }
                    rejectionRetries++;
                    logger.warn("[initiateHeartbeatRPC] Log append retries max limit reached!!");
                    logger.debug("Before :: LogEntry Size->" + this.state.getEntries().size() + " Snapshot Size->" + this.state.getSnapshot().size());
                    if(this.state.getLastApplied() != -1) {
                        this.state.getEntries().subList((int) this.state.getLastApplied(), (int) this.state.getLastLogIndex() + 1).clear();
                        this.state.getEntries().addAll(this.state.getSnapshot());
                        this.state.setLastLogIndex(this.state.getEntries().size() - 1);
                        this.state.setLastApplied(this.state.getLastLogIndex());
                        logger.debug("After :: " + this.state.getEntries().size());
                        logAppendRetries = 0;
                        this.state.getSnapshot().clear();
                        logger.debug("After :: LogEntry Size->" + this.state.getEntries().size() + " Snapshot Size->" + this.state.getSnapshot().size());
                        }
                    }
            } else {
                this.state.getEntries().addAll(this.state.getSnapshot());
                this.state.setLastLogIndex(this.state.getEntries().size() - 1);
                this.state.getSnapshot().clear();
            }
            logger.debug("[initiateHeartbeatRPC] Snapshot :: "+ this.state.getSnapshot());
            logger.debug("[initiateHeartbeatRPC] Entries :: "+ this.state.getEntries());
            logger.debug("[initiateHeartbeatRPC] Current Node is a leader! Reseting the heartbeat timeout to :: "+System.currentTimeMillis());
            this.state.setHeartbeatTrackerTime(System.currentTimeMillis());

            this.state.getEntries().stream().forEach(le ->
                    logger.debug(le.getTerm() + " :: " + le.getCommand().getKey() +" -> "+le.getCommand().getValue()));

            cluster.stream().forEach(serv -> {
                if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                    this.heartBeatExecutor.submit(() -> sendAppendEntries(serv));
                }
            });
        } catch (Exception e) {
            logger.error("[initiateHeartbeatRPC] excp :: "+ e);
        }

    }

    private void sendAppendEntries(Raft.ServerConnect server) {
        logger.debug("[sendAppendEntries] : Sending request to " + server.getServerId() + " at "+System.currentTimeMillis());
        List<Raft.LogEntry> entryToSend = new ArrayList<>();

        Raft.AppendEntriesRequest.Builder requestBuilder = Raft.AppendEntriesRequest.newBuilder();
        try {
            if (this.state.getNodeType() != Role.LEADER) {
                logger.debug("[sendAppendEntries] : Current node is not leader so cant send heartbeats");
                return;
            }
            int followerIndex = this.state.getNextIndex().get(server.getServerId());
            long peerMatchIndex = this.state.getMatchIndex().get(server.getServerId()); //-1
            long currentIndex = this.state.getCommitIndex();
            long currentTerm = this.state.getCurrentTerm();
            String nodeId = this.state.getNodeId();

            if (followerIndex == 0 || this.state.getEntries().isEmpty()) {
                requestBuilder.setLastAppendedLogIndex(0);
                requestBuilder.setLastAppendedLogTerm(-1);
            } else {
                requestBuilder.setLastAppendedLogTerm((this.state.getEntries().get((int) peerMatchIndex)).getTerm());
                requestBuilder.setLastAppendedLogIndex(peerMatchIndex);
            }

            requestBuilder.setTerm(currentTerm); // Is this correct ?
            requestBuilder.setLeaderId(nodeId);

            requestBuilder.setCommitIndex(this.state.getCommitIndex()); // Last Commit Index
            List<Raft.LogEntry> entries = this.state.getEntries();
            logger.debug("[sendAppendEntries] peer match :: " + peerMatchIndex + " : "+this.state.getLastLogIndex());
            logger.debug("[sendAppendEntries] Snapshot :: "+ this.state.getSnapshot());
            logger.debug("[sendAppendEntries] Entries :: "+ this.state.getEntries());
            //ConvertToInt
            for (long i = peerMatchIndex + 1; i <= this.state.getLastLogIndex(); i++) {
                if(entries.size() <= i) {
                    continue;
                }
                entryToSend.add(entries.get((int) i));
            }
            requestBuilder.addAllEntries(entryToSend);
            //requestBuilder.setLeaderLastAppliedIndex(this.state.getLastApplied());
            requestBuilder.setLastAppliedIndex(this.state.getLastApplied());

            logger.debug("[sendAppendEntries] Final Request :: "+ requestBuilder.toString());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("[sendAppendEntries] ex : "+ e);
        }

        logger.debug("[sendAppendEntries] : before call : " + server.getServerId());
        Raft.Endpoint endpoint = server.getEndpoint();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build();

        lock.lock();
        try {
            RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);
            Raft.AppendEntriesResponse response = raftServiceBlockingStub.appendEntries(requestBuilder.build());
            boolean success = response.getSuccess();
            if(this.state.getCurrentTerm() < response.getTerm()){
                this.state.setNodeType(Role.FOLLOWER);
                logger.warn("Got rejected, as my term was lower as a leader. This shouldn't be happening");
            }
            if (!success) {
                long term = response.getTerm();

                if (term == -2) {
                    logger.warn("Follower thinks someone else is leader");
                }

                if (term == -3) {
                    this.state.getNextIndex().set(server.getServerId(), (int) response.getLastMatchIndex() + 1);
                    this.state.getMatchIndex().set(server.getServerId(), (int) response.getLastMatchIndex());
                    logger.warn("We have different prev index, this shouldn't happen in this design , but can happen in future");
                }

                if (term == -4) {
                    logger.warn("We have different term is not corrected : self correct");
                }

                if (term == -5) {
                    logger.warn("Follower commit index more then leader!");
                }
            } else {
                logger.debug("[sendAppendEntries] Post Response");
                this.state.getNextIndex().set(server.getServerId(), (int) response.getLastMatchIndex() + 1);
                this.state.getMatchIndex().set(server.getServerId(), (int) response.getLastMatchIndex());
            }
        } catch (Exception e) {
            logger.error("[sendAppendEntries] after response ex : " + e);
        } finally {
            lock.unlock();
            channel.shutdown();
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

            this.persistentStore.put(requestId, null);
            this.state.getSnapshot().add(logEntry);
            logger.debug("Created request with id :: "+ requestId);
            return 0;
        } finally {
            lock.unlock();
        }

    }

}
