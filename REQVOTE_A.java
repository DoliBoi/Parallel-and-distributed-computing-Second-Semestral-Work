package cz.cvut.fel.agents.pdv.student;

import cz.cvut.fel.agents.pdv.dsand.Message;


class REQVOTE_A extends Message {

    int term;
    boolean isOK;
    
    REQVOTE_A(Integer term, Boolean isOK) {
        this.term = term;
        this.isOK= isOK;
    }

    
}
