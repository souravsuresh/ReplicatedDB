package com.wisc.raft;

import com.wisc.raft.proto.Raft;
import com.wisc.raft.server.Server;
import com.wisc.raft.service.Database;
import com.wisc.raft.service.RaftConsensusService;
import com.wisc.raft.service.ServerClientConnectionService;
import io.grpc.ServerBuilder;
import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RaftServer {

    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);

    /*args[0]  = our Name, args[1] : our port number , args[2] : string of followers*/
    public static void main(String[] args) throws IOException, InterruptedException {
        logger.info("[RaftServer] Starting the main server!!");

        // @TODO :: Take the server ids and command line args
        Database database = new Database("./leveldb");

        List<Raft.ServerConnect> serverList = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            String[] arg = args[i].split("_");
            if (arg[0] == args[0]) continue;
            int serverId = Integer.parseInt(arg[0]);

            Raft.Endpoint endpoint = Raft.Endpoint.newBuilder().setHost(arg[1]).setPort(Integer.parseInt(arg[2])).build();
            Raft.ServerConnect server = Raft.ServerConnect.newBuilder().setServerId(serverId).setEndpoint(endpoint).build();
            serverList.add(server);
        }

        Server raftServer = new Server(args[0], database);
        raftServer.setCluster(serverList);
        ServerClientConnectionService clientConnectionService = new ServerClientConnectionService(raftServer);
        RaftConsensusService raftConsensusService = new RaftConsensusService(raftServer);
//        System.out.println("[Sys Args] "+args.toString());
        // @TODO: RPC initalization etc , change this name
        io.grpc.Server server = ServerBuilder.forPort(Integer.parseInt(args[1])).addService(raftConsensusService).addService(clientConnectionService).build();

        //Start the server
        raftServer.init("localhost", 9000);
        server.start();
        server.awaitTermination();

        // @TODO: health checks if required/ logging the states of server periodically etc
    }
}
