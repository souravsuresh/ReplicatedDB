package com.wisc.raft.service;

import com.wisc.raft.constants.Role;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.RaftServiceGrpc;
import com.wisc.raft.server.Server;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class RaftConsensusService extends RaftServiceGrpc.RaftServiceImplBase {

    Server server;

    public RaftConsensusService(Server server) {
        this.server = server;
    }

    @Override
    public void requestVotes(Raft.RequestVote request, StreamObserver<Raft.ResponseVote> responseObserver) {
        System.out.println("[RequestVoteService] Inside Request Vote Service Call for :: " + request.getCandidateId());
        Raft.ResponseVote.Builder responseBuilder = Raft.ResponseVote.newBuilder()
                .setGrant(false)
                .setTerm(this.server.getState().getCurrentTerm());

        this.server.getLock().lock();
        try {


            if (request.getCandidateId().equals(this.server.getState().getNodeId())) {
                System.out.println("[RequestVoteService] Oops!! Candidate cant be voting itself or something term wise wrong!");
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
            if (request.getTerm() <= this.server.getState().getCurrentTerm()
                    && this.server.getState().getNodeType().equals(Role.LEADER)) {
                System.out.println("[RequestVoteService] Oops!! Candidate cant be voting as you are already a leader");
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }

            long lastLogTerm = this.server.getState().getLastApplied() == -1 ? -1 : this.server.getState().getEntries().get((int)this.server.getState().getLastApplied()).getTerm();

            long lastLogIndex = this.server.getState().getLastApplied() == 0 ? -1 : this.server.getState().getLastApplied();

            if(request.getLeaderLastAppliedTerm()  < lastLogTerm || request.getLeaderLastAppliedIndex() < this.server.getState().getLastApplied()){
                System.out.println("[RequestVoteService] You have bigger Term or more entries than the pot. leader " + " Leader Term : " + request.getLeaderLastAppliedTerm() + " My Term : " + lastLogTerm);
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }

            System.out.println("[RequestVoteService] Current Role :: " + this.server.getState().getNodeType());
            if (request.getTerm() > this.server.getState().getCurrentTerm()) {
                System.out.println("[RequestVoteService] Candidate term :: " + request.getTerm());
                System.out.println("[RequestVoteService] Follower term :: " + this.server.getState().getCurrentTerm());

                // set the current term to the candidates term
                this.server.getState().setCurrentTerm(request.getTerm());
                this.server.getState().setVotedFor(request.getCandidateId());

                System.out.println("[RequestVoteService] I voted for ::" + this.server.getState().getVotedFor());
                this.server.getState().setNodeType(Role.FOLLOWER);  // @TODO if this guy is voting ideally he should step down
                responseBuilder.setGrant(true);
                responseBuilder.setTerm(this.server.getState().getCurrentTerm());
                responseBuilder.setCandidateLastLogIndex(this.server.getState().getLastLogIndex());
                responseBuilder.setCandidateLastAppliedLogIndex(this.server.getState().getLastApplied());
                //responseBuilder.setCandidateLastAppliedTerm(this.server.getState().getEntries())

                System.out.println("RequestVoteService] Successfuly voted! Current Leader :: " +
                        this.server.getState().getVotedFor() + " Current Node Type ::" +
                        this.server.getState().getNodeType());

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            } else {
                System.out.println("[RequestVoteService] My current term is more than asked!!");
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
        } finally {
            this.server.getLock().unlock();
        }
    }

    @Override
    public void appendEntries(Raft.AppendEntriesRequest request, StreamObserver<Raft.AppendEntriesResponse> responseObserver) {
        System.out.println("appendEntries : Follower and my terms is : " + this.server.getState().getCurrentTerm());
        long leaderTerm = request.getTerm();
        long lastIndex = request.getLastAppendedLogIndex();
        String serverID = request.getLeaderId();
        long lastTerm = request.getLastAppendedLogTerm();
        long commitIndex = request.getCommitIndex();
        int indexTracked = (int) request.getIndexTracked();
        Raft.AppendEntriesResponse.Builder responseBuilder = Raft.AppendEntriesResponse.newBuilder();
        this.server.getState().setHeartbeatTrackerTime(System.currentTimeMillis());
        this.server.getLock().lock();
        try {
            if (leaderTerm < this.server.getState().getCurrentTerm()) {
                responseBuilder = responseBuilder.setSuccess(false).setTerm(this.server.getState().getCurrentTerm()); // What ??
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }

            if (leaderTerm >= this.server.getState().getCurrentTerm()) {

                this.server.getState().setNodeType(Role.FOLLOWER);
                this.server.getState().setVotedFor(request.getLeaderId());
                this.server.getState().setCurrentTerm(leaderTerm);
                responseBuilder.setTerm(this.server.getState().getCurrentTerm());

                //I guess check commit and revert in this design TODO
                if (request.getLastAppendedLogIndex() > this.server.getState().getEntries().size()) {
                    //Ideally reject as this would lead to gaps
                    responseBuilder = responseBuilder.setSuccess(false).setTerm(-3); // What ??
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                }

                List<Raft.LogEntry> currentEntries = this.server.getState().getEntries();
                List<Raft.LogEntry> leaderEntries = request.getEntriesList();

                if (request.getLastAppendedLogTerm() != -1 &&
                        this.server.getState().getEntries().size() > request.getLastAppendedLogIndex() &&
                        this.server.getState().getEntries().get((int) this.server.getState().getLastApplied()).getTerm() != request.getLastAppendedLogTerm()) {
                    System.out.println("Rejecting AppendEntries RPC: terms don't agree " );
                    //rollback by sending one at a time
                    responseBuilder = responseBuilder.setSuccess(false).setTerm(-4); // What ??
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                }

                if(this.server.getState().getEntries().size() > (int) request.getLastAppendedLogIndex() + 1) {
                    this.server.getState().getEntries().subList((int) request.getLastAppendedLogIndex() + 1, this.server.getState().getEntries().size()).clear();
                }
                this.server.getState().getEntries().addAll(leaderEntries);
                this.server.getState().setLastApplied(this.server.getState().getEntries().size() - 1);
                long index = this.server.getState().getEntries().size() - 1;

                responseBuilder.setLastMatchIndex(index);
                responseBuilder.setLastMatchTerm(index == -1 ? 0 : this.server.getState().getEntries().get((int) index).getTerm()); //Check this
                responseBuilder.setSuccess(true);

                //Do other stuff
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            }
        } finally {
            System.out.println("[RaftService] Log Entries has " + this.server.getState().getEntries().size() + " entries");
            this.server.getState().getEntries().stream().forEach(le ->
                    System.out.println(le.getTerm() + " :: " + le.getCommand().getKey() +" -> "+le.getCommand().getValue()));
            this.server.getLock().unlock();
        }
    }

}
