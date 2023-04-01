package com.wisc.raft.service;

import com.wisc.raft.constants.Role;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.RaftServiceGrpc;
import com.wisc.raft.server.Server;
import com.wisc.raft.state.NodeState;
import io.grpc.stub.StreamObserver;

public class RaftConsensusService extends RaftServiceGrpc.RaftServiceImplBase {


    Server server;

    public RaftConsensusService(Server server){
        this.server = server;
    }
    //TODO convert long to int
    @Override
    public void requestVotes(Raft.RequestVote request, StreamObserver<Raft.ResponseVote> responseObserver) {
        System.out.println("Inside Request Vote Service Call!!  " +  request.getCandidateId());
        Raft.ResponseVote.Builder responseBuilder = Raft.ResponseVote.newBuilder();

        if (request.getCandidateId().equals(this.server.getState().getNodeId()) || (request.getTerm() <= this.server.getState().getCurrentTerm() && this.server.getState().getNodeType().equals(Role.LEADER))) {
            System.out.println("Ooops!! Candidate cant be voting itself!");
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;

        }
        boolean voted = false;
        System.out.println("Current Role :: "+ this.server.getState().getNodeType());
        if (request.getTerm() > this.server.getState().getCurrentTerm()) {
            System.out.println("Candidate term :: "+ request.getTerm());
            System.out.println("Follower term :: "+ this.server.getState().getCurrentTerm());
            // set the current term to the candidates term
            this.server.getState().setCurrentTerm(request.getTerm());

            //step down if leader or candidate
            if (this.server.getState().getNodeType() != Role.FOLLOWER) {
                this.server.getState().setNodeType(Role.FOLLOWER);
            }
        } else {
            System.out.println("My current term is more than asked!!");
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;

        }
        // @TODO check also based on log entry length
        if (request.getTerm() == this.server.getState().getCurrentTerm() && (this.server.getState().getVotedFor() == null)) {
            //grant vote
            voted = true;
            // @Check :: whether we need to add node UUID here?
            this.server.getState().setVotedFor(request.getCandidateId());
            System.out.println("I voted for ::" + this.server.getState().getVotedFor());

        } else {
            System.out.println("Term mismatch :: request term :" + request.getTerm() + " : current term" + this.server.getState().getCurrentTerm());
            System.out.println("Voted for is screwed up :: Current Voted for -> " + this.server.getState().getVotedFor() + " Request Candidate ID: " + request.getCandidateId());
            System.out.println("Fak you i don't vote for you :: "+ request.getCandidateId());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;

        }

        long curr_term = request.getTerm();
        long lastIndex = request.getLastLogIndex();
        String  serverID = request.getCandidateId();
        long lastTerm = request.getLastLogTerm();

        responseBuilder.setGrant(voted);
        responseBuilder.setTerm(this.server.getState().getCurrentTerm());
        System.out.println("Current Leader :: "+ this.server.getState().getLeaderTerm() + " Current Node Type ::" + this.server.getState().getNodeType());
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(Raft.AppendEntriesRequest request, StreamObserver<Raft.AppendEntriesResponse> responseObserver) {
        long curr_term = request.getTerm();
        long lastIndex = request.getPrevLogIndex();
        String  serverID = request.getLeaderId();
        long lastTerm = request.getPrevLogTerm();
        long commitIndex = request.getCommitIndex();

        Raft.AppendEntriesResponse resp = server.AppendEntries(request);
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }
}
