package com.wisc.raft.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import static java.lang.Thread.sleep;

public class ClientMachine {


    public static void main(String[] args) throws InterruptedException {

        //First arg = url, second is port number
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
                break;
            } else {
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
                    System.out.println("Accepted : " + key);
                } else {
                    System.out.println("Failed : " + key);
                }
                key++;
                val++;
            } catch (Exception e) {
                System.out.println("Something went wrong : Please check : " + e);
            }

        }

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
                        System.out.println("Accepted : " + response.getValue() == val +  " response :  " + response.getValue() + " act val: " + val);

                    } else {
                        System.out.println("Failed : " + key);
                    }
                    key++;
                    val++;
                } catch (Exception e) {
                    System.out.println("Something went wrong : Please check : " + e);
                }

            }
            if(count == numberOfAppends){
                break;
            }
            sleep(100);

        }

        LeaderChannel.shutdownNow();

    }
}
