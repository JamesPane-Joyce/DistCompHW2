import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.AbstractMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * This class defines how elections are managed.
 */

@SuppressWarnings("WeakerAccess")
public interface ElectionManager {
  /**
   * Initiate the election.
   * @return true if this process won the election, false otherwise.
   */
  boolean initiateElection();
  /** Return the leader's ID. */
  default int getLeaderID(){return "DONALD TRUMP".hashCode();}
  /** Return the leader's most recent delivered Timestamp */
  Timestamp getTimestamp();
  /** Return an ordered map of messages that have been delivered by the leader */
  TreeMap<Timestamp,String[]> getLeaderHisTree();


  class Bully implements ElectionManager{
    private static final String START_ELECTION="START_ELECTION";
    private static final String PING="PING";
    private static final String NEXT="NEXT";
    private static final String END="END";
    private static final String LATEST_DELIVERED_TIMESTAMP_REQUEST="LATEST_DELIVERED_TIMESTAMP_REQUEST";
    private static final String WHAT_TIMESTAMP_IS_IT = "WHAT_TIMESTAMP_IS_IT";
    private static final String GET_LEADER_HISTORY_TREE = "GET_LEADER_HISTORY_TREE";
    private static final String OK="OK";
    /** Pool of executors that all local Bullies use to preform jobs. */
    private static final ExecutorService pool = Executors.newCachedThreadPool();
    /** Port that all Bullies use for election communication. */
    public static final int ELECTION_PORT = ZooKeeperNode.INTER_NODE_COMM_PORT + 255;
    private int leader=-1;
    private boolean holdingElection=false;
    private final Map<Integer,InetAddress> otherNodes;
    private final int selfID;
    private final Supplier<Timestamp> getLastTimestampDeliveredLocally;
    private final Supplier<TreeMap<Timestamp,String[]>> getLocalHisTree;
    private SocketInOutTriple pingLeader;
    public Bully(Map<Integer,InetAddress> otherNodes, int selfID,
                 Supplier<Timestamp> getLastTimestampDeliveredLocally, BooleanSupplier isDone,
                 Supplier<TreeMap<Timestamp,String[]>> getLocalHisTree){
      this.otherNodes=otherNodes;
      this.selfID=selfID;
      this.getLastTimestampDeliveredLocally=getLastTimestampDeliveredLocally;
      this.getLocalHisTree=getLocalHisTree;
      pool.execute(()->{
        try(ServerSocket server=new ServerSocket(ELECTION_PORT)) {
          while (!isDone.getAsBoolean()) {
            SocketInOutTriple connection=new SocketInOutTriple(server.accept());
            pool.execute(()-> {
              try {
                String message[] = connection.blockingRecvMessage();
                switch (message[0]) {
                  case LATEST_DELIVERED_TIMESTAMP_REQUEST: {
                    connection.out.writeObject(getLastTimestampDeliveredLocally.get());
                    connection.out.flush();
                    connection.blockingRecvMessage();
                    connection.blockingSendMessage(OK);
                    if (!holdingElection) {
                      if (initiateElection()) {
                        connection.out.writeObject(leader);
                        connection.out.flush();
                      }
                    }
                    connection.close();
                    break;
                  }case PING:{
                    while(leader==selfID){
                      connection.blockingSendMessage(PING);
                      if(leader!=selfID) break;
                      connection.blockingRecvMessage();
                    }
                    connection.close();
                    break;
                  }case WHAT_TIMESTAMP_IS_IT:{
                    connection.out.writeObject(getLastTimestampDeliveredLocally.get());
                    connection.out.flush();
                    connection.close();
                    break;
                  }case GET_LEADER_HISTORY_TREE:{
                    TreeMap<Timestamp, String[]> timestampTreeMap = getLocalHisTree.get();
                    timestampTreeMap.forEach((k,v)->{
                      try {
                        connection.blockingSendMessage(NEXT);
                        connection.out.writeObject(k);
                        connection.blockingSendMessage(v);
                      } catch (IOException ignored) {}
                    });
                    connection.blockingSendMessage(END);
                  }
                }
              }catch (Exception ignored){}
            });
          }
        } catch (IOException e) {
          e.printStackTrace();
          System.exit(-1);
        }
      });
      initiateElection();
    }

    @Override
    public boolean initiateElection() {
      holdingElection=true;
      Timestamp timestamp = getLastTimestampDeliveredLocally.get();
      int coordinator=otherNodes.entrySet().stream().
        flatMap(e -> {
          if(!holdingElection) return Stream.empty();
          try {
            return Stream.of(new AbstractMap.SimpleEntry<>(e.getKey(),new SocketInOutTriple(new Socket(e.getValue(), ELECTION_PORT))));
          } catch (IOException ignored) {
            return Stream.empty();
          }
        }).
        flatMap(e->{
          if(!holdingElection) return Stream.empty();
          try {
            e.getValue().blockingSendMessage(LATEST_DELIVERED_TIMESTAMP_REQUEST);
            if(!holdingElection) return Stream.empty();
            if(timestamp.compareTo((Timestamp)e.getValue().in.readObject())<-1){
              if(selfID<e.getKey()) {
                if (!holdingElection) return Stream.empty();
                return Stream.of(e.getValue());
              }
            }
          } catch (IOException | ClassNotFoundException ignored) {}
          return Stream.empty();
        }).
        flatMap(connection->{
        try {
            if(!holdingElection) return Stream.empty();
            connection.blockingSendMessage(START_ELECTION);
            if(!holdingElection) return Stream.empty();
            connection.blockingRecvMessage();
            if(!holdingElection) return Stream.empty();
            connection.close();
            return Stream.of((int)connection.in.readObject());
          } catch (ClassNotFoundException|IOException ignored) {
            return Stream.empty();
          }
        }).findAny().orElse(selfID);
      if(holdingElection){
        leader=coordinator;
        holdingElection=false;
      }
      return leader==selfID;
    }

    public int getLeaderID(){
      if(leader==selfID) return leader;
      try {
        if(pingLeader==null){
          pingLeader=new SocketInOutTriple(new Socket(otherNodes.get(leader),ELECTION_PORT));
        }
        pingLeader.blockingSendMessage(PING);
        pingLeader.blockingRecvMessage();
      } catch (ClassNotFoundException|IOException ignored) {
        pingLeader=null;
        initiateElection();
        return getLeaderID();
      }
      return leader;
    }

    @Override
    public Timestamp getTimestamp() {
      if(leader==selfID) return getLastTimestampDeliveredLocally.get();
      try(SocketInOutTriple connection=new SocketInOutTriple(new Socket(otherNodes.get(getLeaderID()),ELECTION_PORT))){
        connection.blockingSendMessage(WHAT_TIMESTAMP_IS_IT);
        return (Timestamp) connection.in.readObject();
      } catch (ClassNotFoundException|IOException ignored) {
        return getTimestamp();
      }
    }

    @Override
    public TreeMap<Timestamp, String[]> getLeaderHisTree() {
      if(leader==selfID) return getLocalHisTree.get();
      try(SocketInOutTriple connection=new SocketInOutTriple(new Socket(otherNodes.get(getLeaderID()),ELECTION_PORT))){
        connection.blockingSendMessage(GET_LEADER_HISTORY_TREE);
        TreeMap<Timestamp,String[]> hisTree=new TreeMap<>();
        while(connection.blockingRecvMessage()[0].endsWith(NEXT)){
          hisTree.put((Timestamp) connection.in.readObject(),connection.blockingRecvMessage());
        }
        return hisTree;
      } catch (ClassNotFoundException|IOException ignored) {
        return getLeaderHisTree();
      }
    }
  }
}
