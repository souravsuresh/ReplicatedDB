package com.wisc.raft.service;

import com.wisc.raft.proto.Raft;
import com.wisc.raft.state.NodeState;
import javafx.util.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Database {

    Map<Long , Pair<Long, Raft.LogEntry>> dbentries ;
    public Database(){
        dbentries = new HashMap<>();
        //this.nodeState = nodeState;
    }

    public int commit(Raft.LogEntry logEntry){
        Pair pair = dbentries.put(logEntry.getCommand().getKey(), new Pair(logEntry.getIndex(), logEntry));
        if (Objects.isNull(pair)){
            return -1;
        }
        else{
            return 1;
        }
    }

    public long read(long key){
        Pair<Long, Raft.LogEntry> pair = dbentries.get(key);
        if (Objects.isNull(pair)){
            return -1;
        }
        else{
            return pair.getValue().getCommand().getValue();
        }
    }
}
