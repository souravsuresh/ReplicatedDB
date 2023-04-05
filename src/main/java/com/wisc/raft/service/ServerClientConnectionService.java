package com.wisc.raft.service;

import com.wisc.raft.constants.CommandType;
import com.wisc.raft.constants.Role;
import com.wisc.raft.server.Server;
import io.grpc.stub.StreamObserver;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

public class ServerClientConnectionService extends ServerClientConnectionGrpc.ServerClientConnectionImplBase{

    //Autowire ?
    Server server;
    public ServerClientConnectionService(Server server){
        this.server = server;
    }


    @Override
    public void get(Client.Request request, StreamObserver<Client.Response> res){
        long key = request.getKey();
        String commandType = request.getCommandType();
        Client.Response response = Client.Response.newBuilder().setSuccess(false).setValue(-1).build();
        if (this.server.getState().getNodeType() != Role.LEADER) {
            System.out.println("Cant perform action as this is not leader!!");
            res.onNext(response);
        }
//        else if(commandType.equals(CommandType.GET)){
//            long ret = server.get(key);
//            if(ret != -1){
//                response = Client.Response.newBuilder().setSuccess(true).setValue(ret).build();
//            }
//        }

        res.onCompleted();


    }


    //@TODO : Probably should Long instead of long
    @Override
    public void put(Client.Request request, StreamObserver<Client.Response> res){
        long key = request.getKey();
        long val = request.getValue();
        String commandType = request.getCommandType();
        Client.Response response = Client.Response.newBuilder().setSuccess(false).setValue(-1).build();

        //This shouldn't work
//        if(commandType.equals(CommandType.PUT) || commandType.equals(CommandType.HEARTBEAT)){
//            int ret = server.put(key, val);
//            if(ret != -1){
//                response = Client.Response.newBuilder().setSuccess(true).setValue(ret).build();
//            }
//        }
        res.onNext(response);
        res.onCompleted();

    }
}
