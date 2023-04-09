package com.wisc.raft.basic;

import com.wisc.raft.RaftServer;
import com.wisc.raft.proto.Sample;
import com.wisc.raft.proto.SampleServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class SampleServerClient {

    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);

    private static ScheduledFuture submitObjects;
    private static ScheduledExecutorService submitObjectService;

    static List<Sample.SampleServerConnect> cluster;

    String myHost;
    int port;
    static int id;
    static int key = 0;
    static int value = 0;

    private static ThreadPoolExecutor submitExecutor;

    public SampleServerClient(String arg, List<Sample.SampleServerConnect> serverList) {
        this.id = Integer.parseInt(arg);
        this.cluster = serverList;
    }

    public static void main (String [] args) {
        cluster = new ArrayList<>();
        for (int i = 2; i < args.length-1; i++) {
            String[] arg = args[i].split("_");
            if (arg[0] == args[0]) continue;
            int serverId = Integer.parseInt(arg[0]);

            Sample.SampleEndpoint endpoint =  Sample.SampleEndpoint.newBuilder().setHost(arg[1]).setPort(Integer.parseInt(arg[2])).build();
            Sample.SampleServerConnect server =  Sample.SampleServerConnect.newBuilder().setServerId(serverId).setEndpoint(endpoint).build();
            cluster.add(server);

        }
        submitObjectService = Executors.newSingleThreadScheduledExecutor();
        long time = System.currentTimeMillis();
        Runnable initiateSubmit = () -> submitObject(args[args.length-1],time);

        submitObjects = submitObjectService.scheduleAtFixedRate(initiateSubmit, 0, 160, TimeUnit.MILLISECONDS);
        submitExecutor = new ThreadPoolExecutor(cluster.size(), cluster.size(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

    }

    public static void submitObject(String limit, long time) {

        boolean isRunning = true;
        while (isRunning) {
            long t1 = System.currentTimeMillis();
            List<Sample.SampleLogEntry> alist = new ArrayList<>();
            while(true) {

                Sample.SampleLogEntry logEntry = Sample.SampleLogEntry.newBuilder().setTerm(key).setCommand(Sample.SampleCommand.newBuilder().setKey(key).setValue(value).setCommandType("HelloWorld")).setIndex(String.valueOf(key)).build();
                alist.add(logEntry);
                key++;
                value++;
                logger.info(limit);
                if(t1 - System.currentTimeMillis() > 80 || key == Integer.parseInt(limit)) {
                    break;
                }
            }

            cluster.stream().forEach(cl -> submitExecutor.submit(() -> appendEntries(cl.getEndpoint(), alist)));

            if(key == Integer.parseInt(limit)){
                logger.info(String.valueOf(time));
                isRunning = false;
                submitObjects.cancel(true);
            }

        }

    }

    private static void appendEntries(Sample.SampleEndpoint endpoint, List<Sample.SampleLogEntry> logEntry) {
        try{

        ManagedChannel channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build();
        SampleServiceGrpc.SampleServiceBlockingStub raftServiceBlockingStub = SampleServiceGrpc.newBlockingStub(channel);

        Sample.SampleAppendEntriesRequest req = Sample.SampleAppendEntriesRequest.newBuilder().addAllEntries(logEntry).
                setCommitIndex(1).setTerm(2).setLastAppendedLogIndex(1).setLastAppendedLogTerm(1).setIndexTracked(1).setLeaderId("1").build();

            Sample.SampleAppendEntriesResponse response = raftServiceBlockingStub.appendEntries(req);

            channel.shutdownNow();

        }
        catch(Exception e){
            System.out.println("HW " + e);
        }

    }


}
