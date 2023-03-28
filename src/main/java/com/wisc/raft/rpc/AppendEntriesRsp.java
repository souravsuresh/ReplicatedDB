package com.wisc.raft.rpc;

import com.wisc.raft.log.LogEntry;


public class AppendEntriesRsp {
    
    private int term;
    private boolean success;

    public AppendEntriesRsp(int term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}