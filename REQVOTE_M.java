package cz.cvut.fel.agents.pdv.student;

import cz.cvut.fel.agents.pdv.dsand.Message;

class REQVOTE_M extends Message {
    int term;
    String candidateID;
    int lastIndex;
    int lastTerm;

    REQVOTE_M(int term, String candidateID, int lastIndex, int lastTerm) {
        this.term = term;
        this.candidateID = candidateID;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
    }
}
