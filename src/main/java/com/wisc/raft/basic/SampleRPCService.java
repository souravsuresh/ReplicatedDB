package com.wisc.raft.basic;

import com.wisc.raft.RaftServer;
import com.wisc.raft.proto.Sample;
import com.wisc.raft.proto.SampleServiceGrpc;
import com.wisc.raft.service.Database;
import com.wisc.raft.service.SampleDatabase;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wisc.raft.proto.Client;

import java.util.List;

public class SampleRPCService extends SampleServiceGrpc.SampleServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);

    private SampleDatabase db;
    static int count = 0;

    SampleRPCService(SampleDatabase db){
        this.db = db;
    }

    @Override
    public void appendEntries(Sample.SampleAppendEntriesRequest sampleAppendEntriesRequest, StreamObserver<Sample.SampleAppendEntriesResponse> res){
        List<Sample.SampleLogEntry> entries = sampleAppendEntriesRequest.getEntriesList();
        Sample.SampleAppendEntriesResponse.Builder responseBuilder = Sample.SampleAppendEntriesResponse.newBuilder().setSuccess(false).setTerm(1).setLastMatchTerm(1).setLastMatchIndex(1);

        entries.stream().forEach( en -> {
           int commit = db.commit(en);
            logger.info(count + " :  " + commit +" : " + System.currentTimeMillis());

            if(commit != -1){
                count++;
            }

        });
        Sample.SampleAppendEntriesResponse response = responseBuilder.setSuccess(true).build();
        res.onNext(response);
        res.onCompleted();

    }

}
