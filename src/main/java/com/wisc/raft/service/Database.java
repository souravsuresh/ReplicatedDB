package com.wisc.raft.service;

import com.wisc.raft.proto.Raft;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class Database {

    private String path;
    public DB getDb() {
        return db;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    private DB db;

    public Database(String path) {
        Options options = new Options();
        options.createIfMissing(true);
        this.path = path;
        try {
            db = factory.open(new File(this.path), options);
        } catch (Exception e) {
            System.out.println("Error in Database creation : " + e);
        }
    }

    public static byte[] serialize(Object obj) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        os.flush();
        return out.toByteArray();
    }

    // Deserialize a byte array to an object
    public static Raft.LogEntry deserialize(byte[] data) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return (Raft.LogEntry) is.readObject();
    }

    public int commit(Raft.LogEntry logEntry) {
        byte[] keyBytes = ByteBuffer.allocate(Long.BYTES).putLong(logEntry.getCommand().getKey()).array();
        if (Objects.isNull(keyBytes)) {
            System.out.println("[Database] Key cannot not be serialized");
            return -1;
        }
        try {
            byte[] object = serialize(logEntry);
            if (Objects.isNull(keyBytes)) {
                System.out.println("[Database] LogEntry cannot not be serialized");
                return 1;
            }
            db.put(keyBytes, object);
            return 0;
        } catch (Exception e) {
            System.out.println("[Database] Exception while serializing : " + e);
        }
        return 0;
    }

    public long read(long key) {
        byte[] keyBytes = ByteBuffer.allocate(Long.BYTES).putLong(key).array();
        if (Objects.isNull(keyBytes)) {
            System.out.println("[Database] Object not retrieved");
            return -1;
        }
        byte[] bytes = db.get(keyBytes);
        try {
            Raft.LogEntry logEntry = deserialize(bytes);
            return logEntry.getCommand().getValue();
        } catch (Exception e) {
            System.out.println("[Database] Exception while deserializing : " + e);
        }
        return 0;


    }
}
