package com.wisc.raft.client;

import com.wisc.raft.RaftServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import java.io.IOException;

import static java.lang.Thread.sleep;

public class ClientMachine {
    private static final Logger logger =  LoggerFactory.getLogger(ClientMachine.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        ClientService clientService = new ClientService();

        int appends =  Integer.parseInt(args[2]);
        io.grpc.Server server = ServerBuilder.forPort(Integer.parseInt(args[4], Integer.parseInt(args[5]))).addService(clientService).build();
        server.start();
        long start = System.currentTimeMillis();
        appendSomeEntries(args);
        long mid = System.currentTimeMillis();
        long cp1 = mid-start;
        while(clientService.count != appends){
            continue;
        }
        long cp2 = System.currentTimeMillis() - start;
        logger.info(cp1 + " : " + cp2);
        server.awaitTermination();
    }

    private static void appendSomeEntries(String[] args) throws InterruptedException {
        logger.info(args[0] + " : " + args[1]);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(args[0], Integer.parseInt(args[1])).usePlaintext().build();
        ServerClientConnectionGrpc.ServerClientConnectionBlockingStub serverClientConnectionBlockingStub = ServerClientConnectionGrpc.newBlockingStub(channel);
        Client.MetaDataRequest metaDataRequest = Client.MetaDataRequest.newBuilder().setReqType("LEADER_CONNECT").build();
        String leaderHost;
        int leaderPort;
        while (true) {
            //Shit design - Need to do something else
            Client.MetaDataResponse metaDataResponse = serverClientConnectionBlockingStub.getLeader(metaDataRequest);
            if (metaDataResponse.getSuccess()) {
                leaderHost = metaDataResponse.getHost();
                leaderPort = metaDataResponse.getPort();
                logger.info(leaderHost + " : Leader Info : " + leaderPort);
                break;
            } else {
                logger.debug("Going to sleep :"  + metaDataResponse);
                sleep(500);
            }
        }

        channel.shutdownNow();

        ManagedChannel LeaderChannel = ManagedChannelBuilder.forAddress(leaderHost, leaderPort).usePlaintext().build();
        ServerClientConnectionGrpc.ServerClientConnectionBlockingStub serverClientConnectionBlockingStubLeader = ServerClientConnectionGrpc.newBlockingStub(LeaderChannel);
        int numberOfAppends = Integer.parseInt(args[2]);
        int key = 10;
        int val = 110;
        for (int i = 0; i < numberOfAppends; i++) {
            //TODO put to const
            Client.Request request = Client.Request.newBuilder().setCommandType("PUT").setKey(key).setValue(val).build();
            try {
                Client.Response response = serverClientConnectionBlockingStubLeader.put(request);
                if (response.getSuccess()) {

                    logger.debug("Accepted : " + key);
                } else {
                    logger.warn("Failed : " + key);
                }
                key++;
                val++;
            } catch (Exception e) {
                logger.error("Something went wrong : Please check : " + e);
            }

        }
        int iter = 0;
        while (true) {
            key = 10;
            val = 110;
            int count = 0;
            for (int i = 0; i < numberOfAppends; i++) {
                //TODO put to const
                Client.Request request = Client.Request.newBuilder().setCommandType("GET").setKey(key).build();
                try {
                    Client.Response response = serverClientConnectionBlockingStubLeader.get(request);
                    if (response.getSuccess()) {
                        count++;
                        logger.debug("Accepted : " + (response.getValue() == val) +  " response :  " + response.getValue() + " act val: " + val);

                    } else {
                        logger.warn("Failed : " + key);
                    }
                    key++;
                    val++;
                } catch (Exception e) {
                    logger.error("Something went wrong : Please check : " + e);
                }

            }
            if(count == numberOfAppends){
                break;
            }
            iter++;
            sleep(100);
            logger.debug("Trying :: "+ iter);
        }
        logger.debug("Finally took :: "+ iter);
        LeaderChannel.shutdownNow();

    }
}
