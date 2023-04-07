package com.wisc.raft.service;

import com.wisc.raft.constants.Role;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.server.Server;
import io.grpc.stub.StreamObserver;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import java.util.Optional;

public class ServerClientConnectionService extends ServerClientConnectionGrpc.ServerClientConnectionImplBase{

    //Autowire ?
    Server server;
    public ServerClientConnectionService(Server server){
        this.server = server;
    }

    @Override
    public void getLeader(Client.MetaDataRequest request, StreamObserver<Client.MetaDataResponse> res){
        String reqString = request.getReqType();
        Client.MetaDataResponse.Builder metaDataResponseBuilder = Client.MetaDataResponse.newBuilder();
        if(reqString.equals("LEADER_CONNECT")){
            if(this.server.getState().getNodeType().equals(Role.LEADER)) {
                Optional<Raft.ServerConnect> leaderOpt = this.server.getCluster().stream().filter(serv -> serv.getServerId() != Integer.parseInt(this.server.getState().getNodeId())).findAny();
                if (leaderOpt.isPresent()) {
                    Raft.ServerConnect serverConnect = leaderOpt.get();
                    Client.MetaDataResponse metaDataResponse = metaDataResponseBuilder.setServerId(serverConnect.getServerId()).setPort(serverConnect.getEndpoint().getPort()).setHost(serverConnect.getEndpoint().getHost()).build();
                    res.onNext(metaDataResponse);
                    res.onCompleted();
                }
            }
            else if(this.server.getState().getNodeType().equals(Role.CANDIDATE)){
                System.out.println("[getLeader] : Election going on - Don't disturb");
                Client.MetaDataResponse metaDataResponse = metaDataResponseBuilder.setSuccess(false).build();
                res.onNext(metaDataResponse);
                res.onCompleted();
            }
            else{
                Optional<Raft.ServerConnect> leaderOpt = this.server.getCluster().stream().filter(serv -> serv.getServerId() != Integer.parseInt(this.server.getState().getVotedFor())).findAny();
                if (leaderOpt.isPresent()) {
                    Raft.ServerConnect serverConnect = leaderOpt.get();
                    Client.MetaDataResponse metaDataResponse = metaDataResponseBuilder.setServerId(serverConnect.getServerId()).setPort(serverConnect.getEndpoint().getPort()).setHost(serverConnect.getEndpoint().getHost()).build();
                    res.onNext(metaDataResponse);
                    res.onCompleted();
                }
            }
        }

    }

    @Override
    public void get(Client.Request request, StreamObserver<Client.Response> res){
        long key = request.getKey();
        String commandType = request.getCommandType();
        Client.Response response = Client.Response.newBuilder().setSuccess(false).setValue(-1).build();
        if (this.server.getState().getNodeType() != Role.LEADER) {
            System.out.println("Cant perform action as this is not leader!!");
            res.onNext(response);
            res.onCompleted();
        }

        else if(commandType.equals("GET")){
            long ret = this.server.getValue(key);
            if(ret != -1){
                response = Client.Response.newBuilder().setSuccess(true).setValue(ret).build();
            }

        }
        res.onNext(response);
        res.onCompleted();


    }


    //@TODO : Probably should Long instead of long
    @Override
    public void put(Client.Request request, StreamObserver<Client.Response> res){
        long key = request.getKey();
        long val = request.getValue();
        String commandType = request.getCommandType();
        Client.Response response = Client.Response.newBuilder().setSuccess(false).setValue(-1).build();
        if (this.server.getState().getNodeType() != Role.LEADER) {
            System.out.println("Cant perform action as this is not leader!!");
            res.onNext(response);
            res.onCompleted();
        }

        if(commandType.equals("PUT")){
            int ret = server.putValue(key, val);
            if(ret != -1){
                response = Client.Response.newBuilder().setSuccess(true).setValue(ret).build();
            }
        }
        res.onNext(response);
        res.onCompleted();

    }
}
