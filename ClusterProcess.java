package cz.cvut.fel.agents.pdv.student;

import cz.cvut.fel.agents.pdv.dsand.Message;
import cz.cvut.fel.agents.pdv.dsand.Pair;
import cz.cvut.fel.agents.pdv.raft.RaftProcess;
import cz.cvut.fel.agents.pdv.raft.messages.*;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Vasim ukolem bude naimplementovat (pravdepodobne nejenom) tuto tridu. Procesy v clusteru pracuji
 * s logy, kde kazdy zanam ma podobu mapy - kazdy zaznam v logu by mel reprezentovat stav
 * distribuovane databaze v danem okamziku.
 *
 * Vasi implementaci budeme testovat v ruznych scenarich (viz evaluation.RaftRun a oficialni
 * zadani). Nasim cilem je, abyste zvladli implementovat jednoduchou distribuovanou key/value
 * databazi s garancemi podle RAFT.
 */
public class ClusterProcess extends RaftProcess<Map<String, String>> {

    // ostatni procesy v clusteru
    private final List<String> otherProcessesInCluster;
    // maximalni spozdeni v siti
    private final int networkDelays;

    private Map<String, String> data;
    private State state;
    private int currentTerm; 
    private String vote;
    private List<LOG_E> log;   
    private int commitIndex;

    
    private Map<String, Integer> nextIndex;
   
    private Map<String, Integer> matchIndex;
    private Map<String, Integer> rpcDue;
    private Map<String, Integer> heartBeatDue;
    private int tick;


    private int electionTimer;
    private Random electionTimeout;
    private String currentLeader;
    private Set<String> votesRecieved;
    private final int reallyBigNumber = 999999999;
    private int majority;

    enum State{
      FOLLOWER,
      LEADER,
      CANDIDATE
    }

    private void cleanHashes() {
      votesRecieved = new HashSet<>();
      nextIndex = new HashMap<>();
      rpcDue = new HashMap<>();
      matchIndex = new HashMap<>();
      heartBeatDue = new HashMap<>();
      for (String neighbour : otherProcessesInCluster) {
        nextIndex.put(neighbour, 1);
        matchIndex.put(neighbour, 0);
        rpcDue.put(neighbour, 0);
        heartBeatDue.put(neighbour, 0);
      }
    }

    public ClusterProcess(String id, Queue<Message> inbox, BiConsumer<String, Message> outbox,
                          List<String> otherProcessesInCluster, int networkDelays) {
        super(id, inbox, outbox);
        this.otherProcessesInCluster = otherProcessesInCluster;
        
        if (networkDelays < 1)
            this.networkDelays = 1;
        else
            this.networkDelays = networkDelays;
        data = new HashMap<>();
        currentTerm = 0;
        log = new ArrayList<>();
        commitIndex = 0;
        state = State.FOLLOWER;
        currentLeader = null;
        cleanHashes();

        tick = 0;
        majority = otherProcessesInCluster.size() / 2; //orientacni pojmenovani moc nedava smysl :)
        electionTimeout = new Random();
        electionTimer = tick + 5* networkDelays + electionTimeout.nextInt(15 * this.networkDelays);
    }

    //fce na vraceni epochy urciteho logu
    private int getTermOf(int index) {
        if(index > log.size() || index < 1) return 0;
        return log.get(index-1).term;
    }

    //fce, ktera prijima zpravy a reaguje na ne podle druhu classy
    private void messageHandler(Message m) {
        if (m instanceof REQVOTE_M) {
            REQVOTE_M requestVote = (REQVOTE_M) m;
            
            int lastLogTerm = 0;
            if(log.size() > 0)
                lastLogTerm = log.get(log.size()-1).term;
            //zkontroluju jestli nejsem ve stare epose
            if (currentTerm < requestVote.term) {
                currentTerm = requestVote.term;
                state = State.FOLLOWER;
                vote = null;
                //pokud jsem byl leader nebo se blizil cas, kdybych vyvolal volby resetuju electionTimer
                if (electionTimer < tick || electionTimer == reallyBigNumber) {
                  electionTimer = tick + 5* networkDelays + electionTimeout.nextInt(15 * networkDelays);
                }
            }
            //vote udelime jen pokud jsme ve stejne epose a nas vote neni nebo je pro odesilatele
        //zaroven je posledni epocha v nasem logu nizsi nez posledni epocha requestu nebo se rovna
        //a mame stejne velky log (delam si poznamku, abych mel dobre if, protoze dela velky problemy)
            if (currentTerm == requestVote.term && (vote == null || vote.equals(requestVote.sender)) && (requestVote.lastTerm > lastLogTerm) || (requestVote.lastTerm == lastLogTerm && requestVote.lastIndex >= log.size())) {
                vote = requestVote.sender;
                electionTimer = tick + 5* networkDelays + electionTimeout.nextInt(15 * networkDelays);
                send(requestVote.candidateID, new REQVOTE_A(currentTerm, true));
            }
            //jinak poslu false
            else
            send(requestVote.candidateID, new REQVOTE_A(currentTerm, false));
        }
        else if (m instanceof AppendEntries) {
            AppendEntries appendEntries = (AppendEntries) m;
            boolean success = false;
            int matchingIndex = 0;
            //zkontroluju jestli nejsem ve stare epose
            if (currentTerm < appendEntries.term) {
                currentTerm = appendEntries.term;
                state = State.FOLLOWER;
                vote = null;
                //pokud jsem byl leader nebo se blizil cas, kdybych vyvolal volby resetuju electionTimer
                if (electionTimer < tick || electionTimer == reallyBigNumber) {
                    electionTimer = tick + 5* networkDelays + electionTimeout.nextInt(15 * networkDelays);
                }
            }
            //dostal jsem heartbeat, zkontroluju, jestli je ze spravne epochy, pote resetuju Timer
            if (currentTerm == appendEntries.term) {
                state = State.FOLLOWER;
                currentLeader = appendEntries.leaderId;
                electionTimer = tick + 5* networkDelays + electionTimeout.nextInt(15 * networkDelays);
                //musime si sjednotit logy s leadrem
                if (appendEntries.prevLogIndex == 0 || (appendEntries.prevLogIndex <= log.size() && getTermOf(appendEntries.prevLogIndex) == appendEntries.prevLogTerm)) {
                    success = true;
                    int idx = appendEntries.prevLogIndex;
                    for (int i = 0; i < appendEntries.logEntries.size(); i++) {
                        idx++;
                        if (getTermOf(idx) != appendEntries.logEntries.get(i).term) {
                            while (log.size() > idx - 1)
                                log.remove(log.size()-1);
                            log.add(appendEntries.logEntries.get(i));
                            System.out.println(getId() + " got log from leader");
                        }
                    }
                    matchingIndex = idx;
                    int newCommitIndex = Math.max(commitIndex, appendEntries.leaderCommit);
                    if (newCommitIndex != commitIndex) {
                        for (int i = commitIndex + 1; i <= newCommitIndex; i++) {
                            LOG_E entry = log.get(i-1);
                            if(data.containsKey(entry.action.key))
                                data.put(entry.action.key, data.get(entry.action.key) + entry.action.val);
                            else
                                data.put(entry.action.key, entry.action.val);
                            send(entry.action.clientID, new ServerResponseConfirm(entry.action.reqID));;
                        }
                    }
                    commitIndex = newCommitIndex;
                }
            }
            send(appendEntries.sender, new AppendEntriesAnswer(currentTerm, success, matchingIndex));
        }

        else if (m instanceof REQVOTE_A) {
            //zkontroluju jestli nejsem ve stare epose
            REQVOTE_A requestVoteResponse = (REQVOTE_A) m;
            if (currentTerm < requestVoteResponse.term) {
                currentTerm = requestVoteResponse.term;
                state = State.FOLLOWER;
                vote = null;
                //pokud jsem byl leader nebo se blizil cas, kdybych vyvolal volby resetuju electionTimer
                if (electionTimer < tick || electionTimer == reallyBigNumber) {
                  electionTimer = tick + 5* networkDelays + electionTimeout.nextInt(15 * networkDelays);
                }
          }
          //pridam si hlas pokud vsechno souhlasi
          if (state == State.CANDIDATE && currentTerm == requestVoteResponse.term) {
              rpcDue.put(requestVoteResponse.sender, reallyBigNumber);
              if (requestVoteResponse.isOK) {
                  votesRecieved.add(requestVoteResponse.sender);
              }
          }
        }
        else if (m instanceof AppendEntriesAnswer) {
            AppendEntriesAnswer appendEntriesAnswer = (AppendEntriesAnswer) m;
            //zkontroluju jestli nejsem ve stare epose
            if (currentTerm < appendEntriesAnswer.term) {
                currentTerm = appendEntriesAnswer.term;
                state = State.FOLLOWER;
                vote = null;
                //pokud jsem byl leader nebo se blizil cas, kdybych vyvolal volby resetuju electionTimer
                if (electionTimer < tick || electionTimer == reallyBigNumber) {
                  electionTimer = tick + 5* networkDelays + electionTimeout.nextInt(15 * networkDelays);
                }
          }
          //jako leader si u sebe poznamenam jestli jsou logy ostatnich serveru v poradku, jinak postupuju dal v doplnovani
          if (state == State.LEADER && currentTerm == appendEntriesAnswer.term) {
              if (appendEntriesAnswer.isOk) {
                  matchIndex.put(appendEntriesAnswer.sender, Math.max(matchIndex.get(appendEntriesAnswer.sender), appendEntriesAnswer.matchIndex));
                  nextIndex.put(appendEntriesAnswer.sender, appendEntriesAnswer.matchIndex + 1);
              } else {
                  nextIndex.put(appendEntriesAnswer.sender, Math.max(1, nextIndex.get(appendEntriesAnswer.sender) - 1));
              }
              rpcDue.put(appendEntriesAnswer.sender, 0);
          }
        }
        else if (m instanceof ClientRequestWithContent) {
            ClientRequestWithContent<IOperation, Pair<String, String>> clientRequest = (ClientRequestWithContent<IOperation, Pair<String, String>>) m;
           
           //kdyz jsem leader confirmuju a pridavam do logu
            if (state == State.LEADER) {
                Pair<String, String> content = (Pair<String, String>) clientRequest.getContent();
    
                    REQ_M req = new REQ_M(clientRequest.sender, clientRequest.getRequestId(), clientRequest.getOperation(), content.getFirst(), content.getSecond());
                    if (log.stream().map(x->x.action.reqID).collect(Collectors.toSet()).contains(req.reqID)) return;
                    log.add(new LOG_E(currentTerm, req));
                    System.out.println(getId() + " got log from client and is leader.");
                    //nejsem leader, musim presmerovat klienta na leadra
            } else {
                send(clientRequest.sender, new ServerResponseLeader(clientRequest.getRequestId(), getCurrentLeader()));
            }
        }

        else if (m instanceof ClientRequestWhoIsLeader) {
            ClientRequestWhoIsLeader clientRequest = (ClientRequestWhoIsLeader) m;
            send(clientRequest.sender, new ServerResponseLeader(clientRequest.getRequestId(), getCurrentLeader()));;
        }

    }


    @Override
    public Optional<Map<String, String>> getLastSnapshotOfLog() {

        // komentar viz deklarace
        return Optional.of(data);
    }

    @Override
    public String getCurrentLeader() {

        // komentar viz deklarace
        return currentLeader;
    }


    @Override
    public void act() {

        // doimplementuje metodu act() podle RAFT

        // krome vlastnich zprav (vasich trid), dostavate typy zprav z balicku raft.messages s rodicem
        // ClientRequest, tak si je projdete, at vite, co je ucelem a obsahem jednotlivych typu.
        // V pripade ClientRequestWithContent dostavate zpravu typu
        // ClientRequestWithContent<StoreOperationEnums, Pair<String, String>>, kde StoreOperationEnums
        // je operace, kterou mate udelat s obsahem paru Pair<String, String>, kde prvni hodnota
        // paru je klic (nikdy neni prazdny) a druha je hodnota (v pripade pozadavku GET je prazdny)

        // dejte si pozor na jednotlive akce podle RAFT. S klientem komunikujte vyhradne pomoci zprav
        // typu ServerResponse v messages

        // na pozadavky klientu odpovidate zpravami typu ServerResponse viz popis podtypu v messages.
        // !!! V TOMTO PRIPADE JE 'requestId' ROVNO POZADAVKU KLIENTA, NA KTERY ODPOVIDATE !!!

        // dalsi podrobnosti naleznete na strance se zadanim


        tick++;

        if ((state == State.FOLLOWER || state == State.CANDIDATE) && electionTimer < tick){
            //poustim nove volby, kdyz mi neprisel heartbeat
            System.out.println(getId() + " started a vote.");
            electionTimer = tick + 5* networkDelays + electionTimeout.nextInt(15 * networkDelays);
            currentTerm++;
            vote = getId();
            state = State.CANDIDATE;
            cleanHashes();
        }

        if (state == State.CANDIDATE && votesRecieved.size() + 1 > majority) {
            //stanu se lidrem, pokud mam dostatek hlasu
            System.out.println(getId() + " is leader now");
            state = State.LEADER;
            currentLeader = getId();
            cleanHashes();
            for (String neighbour : otherProcessesInCluster) {
                nextIndex.put(neighbour, log.size() + 1);
                rpcDue.put(neighbour, reallyBigNumber);
                heartBeatDue.put(neighbour, 0);
            }
            electionTimer = reallyBigNumber;
        }

        //v teto fazi confirmuju logy a zapisuji si je do dat, zaroven pak posilam uzivateli confirm, kdyz vsehcno probehne jak ma
        List<Integer> list = new ArrayList<>(matchIndex.values());
        list.sort(Comparator.comparing(Integer::intValue));
        int n = list.get(majority);
        LOG_E entry = null;
        if (state == State.LEADER && getTermOf(n) == currentTerm) {
            while(commitIndex != n) {
                commitIndex++;
                if (commitIndex > log.size() || commitIndex < 1)
                entry = null;
                else
                entry = log.get(commitIndex-1);
                if(data.containsKey(entry.action.key))
                   data.put(entry.action.key, data.get(entry.action.key) + entry.action.val);
                else
                  data.put(entry.action.key, entry.action.val);
                send(entry.action.clientID, new ServerResponseConfirm(entry.action.reqID));
            }
            System.out.println(getId() + " send confirm");
        }

        //komunikace s ostatnimi servery
        for (String server : otherProcessesInCluster) {
             //pokud jsem candidat rozeslu prosbu o hlas do voleb
            if (state == State.CANDIDATE && rpcDue.get(server) <= tick){
                int lastLogTerm = 0;
                if(log.size() > 0)
                lastLogTerm = log.get(log.size()-1).term;
                rpcDue.put(server, 3 * networkDelays);
                send(server, new REQVOTE_M(currentTerm, getId(), log.size(), lastLogTerm));
            }

            //pokud jsem leader a potrebuju poslat heartbeat nebo mam pro jiny server zpravu poslu appendEntries
            if (state == State.LEADER && (heartBeatDue.get(server) <= tick || (nextIndex.get(server) <= log.size() && rpcDue.get(server) <= tick))) {
                int prevIndex = nextIndex.get(server) - 1;
                int lastIndex = Math.min(prevIndex + 1, log.size());
                if (matchIndex.get(server) + 1 < nextIndex.get(server)) {
                    lastIndex = prevIndex;
                }
                List<LOG_E> logEntries = new ArrayList<>(log.subList(prevIndex, lastIndex));
                send(server, new AppendEntries(currentTerm, getId(), prevIndex, getTermOf(prevIndex), logEntries, Math.min(commitIndex, lastIndex)));
                rpcDue.put(server, tick + 2*networkDelays);
                heartBeatDue.put(server, tick + networkDelays * 2 - 1);
            }
        }

        // zpracovani zprav
        for (Message m : inbox) {
            messageHandler(m);
        }

        // na zaver vycistim schranku
        inbox.clear();
    }

}
