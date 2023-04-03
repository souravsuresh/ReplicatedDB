package com.wisc.raft.service;

import com.wisc.raft.constants.Role;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.RaftServiceGrpc;
import com.wisc.raft.server.Server;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class RaftConsensusService extends RaftServiceGrpc.RaftServiceImplBase {


    Server server;

    public RaftConsensusService(Server server){
        this.server = server;
    }
    //TODO convert long to int
    @Override
    public void requestVotes(Raft.RequestVote request, StreamObserver<Raft.ResponseVote> responseObserver) {
        System.out.println("[RequestVoteService] Inside Request Vote Service Call for :: " +  request.getCandidateId());
        Raft.ResponseVote.Builder responseBuilder = Raft.ResponseVote.newBuilder().setGrant(false).setTerm(this.server.getState().getCurrentTerm());

        if (request.getCandidateId().equals(this.server.getState().getNodeId()) || (request.getTerm() <= this.server.getState().getCurrentTerm() && this.server.getState().getNodeType().equals(Role.LEADER))) {
            System.out.println("[RequestVoteService] Oops!! Candidate cant be voting itself or something term wise wrong!");
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;

        }
        System.out.println("[RequestVoteService] Current Role :: "+ this.server.getState().getNodeType());
        if (request.getTerm() > this.server.getState().getCurrentTerm()) {
            System.out.println("[RequestVoteService] Candidate term :: "+ request.getTerm());
            System.out.println("[RequestVoteService] Follower term :: "+ this.server.getState().getCurrentTerm());
            // set the current term to the candidates term
            this.server.getState().setCurrentTerm(request.getTerm());
            // @Check :: whether we need to add node UUID here?
            this.server.getState().setVotedFor(request.getCandidateId());
            System.out.println("[RequestVoteService] I voted for ::" + this.server.getState().getVotedFor());
            this.server.getState().setNodeType(Role.FOLLOWER);  // @TODO if this guy is voting ideally he should step down
            responseBuilder.setGrant(true);
            responseBuilder.setTerm(this.server.getState().getCurrentTerm());
            System.out.println("RequestVoteService] Successfuly voted! Current Leader :: "+ this.server.getState().getVotedFor() + " Current Node Type ::" + this.server.getState().getNodeType());

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            //step down if leader or candidate
        } else {
            System.out.println("[RequestVoteService] My current term is more than asked!!");
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }
        // @TODO check also based on log entry length 
        // Note the 2nd condition is checking either NULL(case when voting for self not started) and started self voting


//        long curr_term = request.getTerm();
//        long lastIndex = request.getLastLogIndex();
//        String  serverID = request.getCandidateId();
//        long lastTerm = request.getLastLogTerm();
//

    }

    @Override
    public void appendEntries(Raft.AppendEntriesRequest request, StreamObserver<Raft.AppendEntriesResponse> responseObserver) {
        System.out.println("appendEntries : Follower and my terms is : " +  this.server.getState().getCurrentTerm());
        long leaderTerm = request.getTerm();
        long lastIndex = request.getPrevLogIndex();
        String  serverID = request.getLeaderId();
        long lastTerm = request.getPrevLogTerm();
        long commitIndex = request.getCommitIndex();
        int indexTracked = (int) request.getIndexTracked();
        Raft.AppendEntriesResponse.Builder responseBuilder = Raft.AppendEntriesResponse.newBuilder();

//        if(!serverID.equals(this.server.getState().getLeaderId())){
//
//            responseBuilder = responseBuilder.setSuccess(false).setTerm(-2); // What ??
//            responseObserver.onNext(responseBuilder.build());
//            responseObserver.onCompleted();
//            return;
//        }

        if (leaderTerm < this.server.getState().getCurrentTerm()) {
            responseBuilder = responseBuilder.setSuccess(false).setTerm(-1); // What ??
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        if(leaderTerm >= this.server.getState().getCurrentTerm()) {

            this.server.getState().setCurrentTerm(leaderTerm);
            responseBuilder.setTerm(this.server.getState().getCurrentTerm());
            if (this.server.getState().getNodeType().equals(Role.CANDIDATE) || this.server.getState().getNodeType().equals(Role.LEADER)) {
                this.server.downGrade(Role.FOLLOWER);
            }
            //DO we need to track who is the leader ???
            //Only accept resp from leaders

            //I guess check commit and revert in this design TODO
            if (request.getPrevLogIndex() > this.server.getState().getEntries().size()) {
                //Ideally reject as this would lead to gaps
                responseBuilder = responseBuilder.setSuccess(false).setTerm(-3); // What ??
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }

            List<Raft.LogEntry> currentEntries = this.server.getState().getEntries();
            List<Raft.LogEntry> leaderEntries = request.getEntriesList();

            if (request.getPrevLogIndex() != -1 && this.server.getState().getEntries().get((int) request.getPrevLogIndex()).getTerm() != request.getPrevLogTerm()) {
                System.out.println("Rejecting AppendEntries RPC: terms don't agree");
                //rollback by sending one at a time
                responseBuilder = responseBuilder.setSuccess(false).setTerm(-4); // What ??
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }

            //Check if HeartBeat
//            if (request.getEntriesCount() == 0) {
//                System.out.println("HB");
//                responseBuilder.setSuccess(true);
//                responseBuilder.setTerm(this.server.getState().getCurrentTerm());
//                responseBuilder.setLastMatchIndex(this.server.getState().getCommitIndex());
//                responseBuilder.setLastMatchTerm(this.server.getState().getCurrentTerm());
//                //Do other stuff
//                responseObserver.onNext(responseBuilder.build());
//                responseObserver.onCompleted();
//                return;
//            }
            int i;
            for (i = 0; i < leaderEntries.size(); i++) {
                if (indexTracked < currentEntries.size()) {
                    if (leaderEntries.get(i).getTerm() == currentEntries.get(indexTracked).getTerm()) {
                        indexTracked++;

                    }
                } else {
                    this.server.getState().getEntries().add(leaderEntries.get(i));
                    indexTracked++;

                }

            }
            responseBuilder.setLastMatchIndex(leaderEntries.size()-1);
            responseBuilder.setLastMatchTerm(leaderEntries.size() != 0 ? leaderEntries.get(i).getTerm() : this.server.getState().getCurrentTerm()); //Check this
            responseBuilder.setSuccess(true);
            //Do other stuff

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }
}
