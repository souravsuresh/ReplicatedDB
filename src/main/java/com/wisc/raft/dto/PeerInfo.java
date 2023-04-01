package com.wisc.raft.dto;

import com.wisc.raft.proto.Raft;

public class PeerInfo {
    private long nextIndex;
    private long matchCommitIndex;



    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchCommitIndex() {
        return matchCommitIndex;
    }

    public void setMatchCommitIndex(long matchCommitIndex) {
        this.matchCommitIndex = matchCommitIndex;
    }

    public Raft.ServerConnect getServerConnect() {
        return serverConnect;
    }

    public void setServerConnect(Raft.ServerConnect serverConnect) {
        this.serverConnect = serverConnect;
    }

    private Raft.ServerConnect serverConnect;

    public PeerInfo(long nextIndex, long matchCommitIndex, Raft.ServerConnect serverConnect) {
        this.nextIndex = nextIndex;
        this.matchCommitIndex = matchCommitIndex;
        this.serverConnect = serverConnect;
    }
}
