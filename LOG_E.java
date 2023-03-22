package cz.cvut.fel.agents.pdv.student;

import java.io.Serializable;

class LOG_E implements Serializable {
    final int term;
    final REQ_M action;

    LOG_E(int term, REQ_M action){
        this.term = term;
        this.action = action;
    }
}
