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

    // @TODO: for now maintaining HashMap!! Update this to db updates
    private HashMap<Long, Long> kvStore = new HashMap<>();
    private NodeState state;
    private List<Raft.ServerConnect> cluster;

    //Threading
    private ScheduledExecutorService electionExecutorService;
    private ScheduledExecutorService heartbeatExecutorService;

    private ScheduledFuture electionScheduler;
    private ScheduledFuture heartBeatScheduler;

    private ThreadPoolExecutor electionExecutor;
    private ThreadPoolExecutor heartBeatExecutor;

    public Lock getLock() {
        return lock;
    }

    public void setLock(Lock lock) {
        this.lock = lock;
    }

    private Lock lock;


    private RaftConsensusService raftConsensusService;

    public Server(String nodeId) {
        this.state = new NodeState(nodeId);
    }

    public void init() {
        lock = new ReentrantLock();
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

        this.electionExecutor = new ThreadPoolExecutor(cluster.size(), cluster.size(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        this.heartBeatExecutor = new ThreadPoolExecutor(cluster.size(), cluster.size(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        electionExecutorService =  Executors.newSingleThreadScheduledExecutor();
        electionScheduler = electionExecutorService.scheduleAtFixedRate(initiateElectionRPCRunnable, 1, 5, TimeUnit.SECONDS);
         // electionScheduler = electionExecutorService.scheduleAtFixedRate(initiateElectionRPCRunnable, 1L, (long) (100 + random.nextDouble() * ELECTION_TIMEOUT_INTERVAL), TimeUnit.SECONDS);
        heartbeatExecutorService =  Executors.newSingleThreadScheduledExecutor();
        heartBeatScheduler = heartbeatExecutorService.scheduleAtFixedRate(initiateHeartbeatRPCRunnable, 5, 2, TimeUnit.SECONDS);
    }

    public void initiateElectionRPC() {
        Raft.RequestVote.Builder requestBuilder = Raft.RequestVote.newBuilder();

        lock.lock();
        try {
            if(this.state.getHeartbeatTrackerTime() != 0 && System.currentTimeMillis() > (this.state.getHeartbeatTrackerTime() + 2 * 1000) ) {
                this.state.setVotedFor(null);
                this.state.setNodeType(Role.FOLLOWER);
            }
            if (this.state.getNodeType().equals(Role.LEADER)) {
                System.out.println("[initiateElectionRPC]  Already a leader! So not participating in Election!");
                //Random rn = new Random();
                for(int i=0;i<1;i++){
                    this.state.getEntries().add(Raft.LogEntry.newBuilder().setCommand(Raft.Command.newBuilder().setValue(random.nextInt(10)).setKey(random.nextInt(10)).build()).setTerm(this.state.getCurrentTerm()).setIndex("Bolimaga").build());
                }
                return;
            }
            if (!Objects.isNull(this.state.getVotedFor()) && this.state.getNodeType().equals(Role.FOLLOWER)) {
                System.out.println("[initiateElectionRPC]  Already voted ! So not participating in Election! : " + this.state.getVotedFor());
                return;
            }
//            if(this.state.getHeartbeatTrackerTime() != 0 && System.currentTimeMillis() > this.state.getHeartbeatTrackerTime() + HEARTBEAT_TIMEOUT_INTERVAL )

            System.out.println("[initiateElectionRPC] Starting the voting process");
            this.state.setVotedFor(this.state.getNodeId());
            this.state.setNodeType(Role.CANDIDATE);
            this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
            requestBuilder.setCandidateId(this.state.getNodeId());
            requestBuilder.setTerm(this.state.getCurrentTerm());
            requestBuilder.setLastLogIndex(this.state.getCommitIndex());
            long lastLogTerm = this.state.getCommitIndex() == 0 ? 0 : this.state.getEntries().get(this.state.getEntries().size() - 1).getTerm();
            requestBuilder.setLastLogTerm(lastLogTerm);
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
                        this.state.setNodeType(Role.LEADER);
                    }
                    System.out.println("[RequestVoteWrapper] Number of Votes : " + this.state.getTotalVotes());
                } finally {
                    lock.unlock();
                }
            } else {
                System.out.println("[RequestVoteWrapper] Not granted by :: " + endpoint.getPort() + " Response :: " + responseVote.getTerm() + " Current :: " + this.state.getCurrentTerm());
            }
            channel.shutdownNow();

        }catch (Exception ex) {
                System.out.println("[RequestVoteWrapper] Server might not be up!! "+ ex);
        }


    }

    public void initiateHeartbeatRPC() {
        lock.lock();
        try {
            if (!this.state.getNodeType().equals(Role.LEADER)) {
                System.out.println("[initiateHeartbeatRPC] Not a leader! So not participating in HeartBeat!");
                return;
            }
        }
        catch(Exception e){
            System.out.println("[initiateHeartbeatRPC] " + e);
        }
        finally {
            lock.unlock();
        }
        try {
            System.out.println("[RaftService] Log Entries has " + this.state.getEntries().size() + " entries");
            this.state.getEntries().stream().forEach(le ->
                    System.out.println(le.getTerm() + " :: " + le.getCommand().getKey() +" -> "+le.getCommand().getValue()));

            cluster.stream().forEach(serv -> {
                if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                    this.heartBeatExecutor.submit(() -> sendAppendEntries(serv));
                }
            });
            this.state.setHeartbeatTrackerTime(System.currentTimeMillis());
        } catch (Exception e) {
            System.out.println("[initiateHeartbeatRPC] excp :: "+ e);
        }

    }

    private void sendAppendEntries(Raft.ServerConnect server) {

        System.out.println("[sendAppendEntries] : Sending request to " + server.getServerId());
        List<Raft.LogEntry> entryToSend = new ArrayList<>();

        Raft.AppendEntriesRequest.Builder requestBuilder = Raft.AppendEntriesRequest.newBuilder();

        lock.lock();
        try {
            if (this.state.getNodeType() != Role.LEADER) {
                System.out.println("[sendAppendEntries] : Current node is leader so cant send heartbeats");
                return;
            }
            int followerIndex = this.state.getNextIndex().get(server.getServerId());
            long peerMatchIndex = this.state.getMatchIndex().get(server.getServerId()); //-1
            long currentIndex = this.state.getCommitIndex();
            long currentTerm = this.state.getCurrentTerm();
            String nodeId = this.state.getNodeId();
            if (currentIndex < followerIndex) {
                System.out.println("[sendAppendEntries] : Current index : "+ currentIndex+" Follower Index : "+ followerIndex +"This cannot happen right ??");
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
            System.out.println("[sendAppendEntries] Final Request :: "+ requestBuilder.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("[sendAppendEntries] ex : "+ e);
        } finally {
            lock.unlock();
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
                return;
            }
        } catch (Exception e) {
            System.out.println("[sendAppendEntries] after response ex : "+ e);
        } finally {
            lock.unlock();
        }
    }

}
