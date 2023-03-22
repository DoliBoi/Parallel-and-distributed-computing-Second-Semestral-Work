package cz.cvut.fel.agents.pdv.student;

import cz.cvut.fel.agents.pdv.raft.messages.IOperation;

import java.io.Serializable;

class REQ_M implements Serializable {
    final String clientID;
    final String reqID;
    final String operation;
    final String key;
    final String val;

    REQ_M(String clientID, String reqID, IOperation operation, String key, String val){
        this.clientID = clientID;
        this.reqID = reqID;
        this.operation = operation.toString();
        this.key = key;
        this.val = val;
    }
}
