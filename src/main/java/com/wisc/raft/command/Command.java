package com.wisc.raft.command;

import com.wisc.raft.constants.CommandType;

public class Command {
    private int key;
    private int value;
    private CommandType commandType = CommandType.HEARTBEAT;

    public Command(int key, int value) {
        this.commandType = CommandType.PUT;
        this.key = key;
        this.value = value;
    }

    public int getKey() {
        return key;
    }
    public void setKey(int key) {
        this.key = key;
    }
    public int getValue() {
        return value;
    }
    public void setValue(int value) {
        this.value = value;
    }
    public CommandType getCommandType() {
        return commandType;
    }
    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    
}
