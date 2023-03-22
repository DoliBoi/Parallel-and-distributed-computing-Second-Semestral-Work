package cz.cvut.fel.agents.pdv.student;

import cz.cvut.fel.agents.pdv.dsand.Message;

class AppendEntriesAnswer extends Message {
    final int matchIndex;
    int term;
    boolean isOk;

    AppendEntriesAnswer(Integer term, boolean isOk, int matchIndex) {
        this.term = term;
        this.isOk = isOk;
        this.matchIndex = matchIndex;
    }
}