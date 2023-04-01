package com.wisc.raft.service;

import com.wisc.raft.constants.Role;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.RaftServiceGrpc;
import com.wisc.raft.server.Server;
import io.grpc.stub.StreamObserver;

public class RaftConsensusService extends RaftServiceGrpc.RaftServiceImplBase {


    Server server;

    public RaftConsensusService(Server server){
        this.server = server;
    }
    //TODO convert long to int
    @Override
    public void requestVotes(Raft.RequestVote request, StreamObserver<Raft.ResponseVote> responseObserver) {
        System.out.println("[RequestVoteService] Inside Request Vote Service Call for :: " +  request.getCandidateId());
        Raft.ResponseVote.Builder responseBuilder = Raft.ResponseVote.newBuilder();

        if (request.getCandidateId().equals(this.server.getState().getNodeId()) || (request.getTerm() <= this.server.getState().getCurrentTerm() && this.server.getState().getNodeType().equals(Role.LEADER))) {
            System.out.println("[RequestVoteService] Ooops!! Candidate cant be voting itself!");
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;

        }
        boolean voted = false;
        System.out.println("[RequestVoteService] Current Role :: "+ this.server.getState().getNodeType());
        if (request.getTerm() > this.server.getState().getCurrentTerm()) {
            System.out.println("[RequestVoteService] Candidate term :: "+ request.getTerm());
            System.out.println("[RequestVoteService] Follower term :: "+ this.server.getState().getCurrentTerm());
            // set the current term to the candidates term
            this.server.getState().setCurrentTerm(request.getTerm());

            //step down if leader or candidate
            if (this.server.getState().getNodeType() != Role.FOLLOWER) {
                this.server.getState().setNodeType(Role.FOLLOWER);
            }
        } else {
            System.out.println("[RequestVoteService] My current term is more than asked!!");
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;

        }
        // @TODO check also based on log entry length 
        // Note the 2nd condition is checking either NULL(case when voting for self not started) and started self voting
        if (request.getTerm() == this.server.getState().getCurrentTerm() && 
                (this.server.getState().getVotedFor() == null || this.server.getState().getVotedFor().equals(this.server.getState().getNodeId()))) {
            //grant vote
            voted = true;
            // @Check :: whether we need to add node UUID here?
            this.server.getState().setVotedFor(request.getCandidateId());
            System.out.println("[RequestVoteService] I voted for ::" + this.server.getState().getVotedFor());
            this.server.getState().setNodeType(Role.FOLLOWER);  // @TODO if this guy is voting ideally he should step down

        } else {
            System.out.println("[RequestVoteService] Term mismatch :: Requested term :" + request.getTerm() + " : Current Node term :: " + this.server.getState().getCurrentTerm());
            System.out.println("[RequestVoteService] Voted for is screwed up :: Current Voted for -> " + this.server.getState().getVotedFor() + " Request Candidate ID: " + request.getCandidateId());
            System.out.println("Node :: "+this.server.getState().getNodeId()+" didnt vote for :: "+ request.getCandidateId());
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
        System.out.println("RequestVoteService] Successfuly voted! Current Leader :: "+ this.server.getState().getVotedFor() + " Current Node Type ::" + this.server.getState().getNodeType());
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
