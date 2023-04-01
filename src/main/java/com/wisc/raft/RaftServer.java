package com.wisc.raft;

import com.wisc.raft.proto.Raft;
import com.wisc.raft.server.Server;
import com.wisc.raft.service.RaftConsensusService;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class RaftServer {
    /*args[0]  = our Name, args[1] : our port number , args[2] : string of followers*/
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Inside Main bitches");
        // @TODO :: Take the server ids and command line args


        List<Raft.ServerConnect> serverList = new ArrayList<>();
        for (int i=2;i<args.length;i++) {
            String [] arg = args[i].split("_");
            int serverId = Integer.parseInt(arg[0]);
            Raft.Endpoint endpoint = Raft.Endpoint.newBuilder().setHost(arg[1]).setPort(Integer.parseInt(arg[2])).build();
            Raft.ServerConnect server = Raft.ServerConnect.newBuilder().setServerId(serverId).setEndpoint(endpoint).build();
            serverList.add(server);
        }

        Server raftServer = new Server(args[0]);
        raftServer.setCluster(serverList);
        //ServerClientConnectionService clientConnectionService = new ServerClientConnectionService(raftServer);
        RaftConsensusService raftConsensusService = new RaftConsensusService(raftServer);

        // @TODO: RPC initalization etc , change this name
        io.grpc.Server server = ServerBuilder.forPort(Integer.parseInt(args[1])).addService(raftConsensusService).build();
        //Start the server
        raftServer.start();
        server.start();
        server.awaitTermination();


        // @TODO: health checks if required/ logging the states of server periodically etc
    }
}
