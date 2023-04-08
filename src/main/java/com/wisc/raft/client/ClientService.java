package com.wisc.raft.client;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import java.util.HashSet;
import java.util.Set;

public class ClientService extends ServerClientConnectionGrpc.ServerClientConnectionImplBase{
    public static int count = 0;
    @Override
    public void talkBack(Client.StatusUpdate req, StreamObserver<Empty> response){
        if(req.getReturnVal()){
            count++;
        }
        response.onNext(Empty.getDefaultInstance());
        response.onCompleted();
    }
}
