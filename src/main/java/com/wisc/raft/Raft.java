package com.wisc.raft;

import java.util.UUID;
import com.wisc.raft.server.Server;

public class Raft {

    public static void main(String[] args) {
        
        // @TODO :: Take the server ids and command line args
        Server raftNode = new Server(UUID.randomUUID().toString());

        // @TODO: RPC initalization etc
        
        // @TODO: health checks if required/ logging the states of server periodically etc
    }
}
