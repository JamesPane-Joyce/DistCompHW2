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
 * This interface defines how elections are managed.
 */
@SuppressWarnings("WeakerAccess")
public interface ElectionManager {
  /**
   * Initiate the election.
   * @return true if this process won the election, false otherwise.
   */
  boolean initiateElection();

  /**
   * Get the ID of the node of the Leader.
   * Sets up an election if there is no leader, or if a pinging of the leader fails.
   * Returns the ID of the leader or recurses forever.
   * @return The ID of the leader.
   */
  default int getLeaderID(){return "DONALD TRUMP".hashCode();}

  /**
   * Return an ordered map of messages that have been delivered by the leader.
   * Sets up an election if there is no leader, or if a pinging of the leader fails.
   * Returns an ordered map of messages of the leader or recurses forever.
   * @return An ordered map of Timestamps to messages, earliest first.
   */
  TreeMap<Timestamp,String[]> getLeaderHisTree();

  /**
   * A facsimile of the BullyManager Algorithm but uses TCP to loosen the need for timers .
   */
  class BullyManager implements ElectionManager{
    /** A String to be sent indicating the start of an election. */
    private static final String START_ELECTION="START_ELECTION";
    /** A String to be sent over and back to ensure the leader isn't ded yet. */
    private static final String PING="PING";
    /** A String to be sent over indicating there is more data coming. */
    private static final String NEXT="NEXT";
    /** A String to be sent over indicating there is no more data coming. */
    private static final String END="END";
    /** A String to be sent over indicating a request for the latest timestamp delivered on the other machine.
     * Really indicates interest in an Election. */
    private static final String LATEST_DELIVERED_TIMESTAMP_REQUEST="LATEST_DELIVERED_TIMESTAMP_REQUEST";
    /** A String to be sent over indicating a request for the history of delivered messages. */
    private static final String GET_LEADER_HISTORY_TREE = "GET_LEADER_HISTORY_TREE";
    /** A String to be sent over indicating the acknowledgement of and election*/
    private static final String OK="OK";
    /** Pool of executors that all local Bullies use to preform jobs. */
    private static final ExecutorService pool = Executors.newCachedThreadPool();
    /** Port that all Bullies use for election communication. */
    public static final int ELECTION_PORT = ZooKeeperNode.INTER_NODE_COMM_PORT + 255;
    /** The ID of the most recently elected leader. */
    private int leader=-1;
    /** Whether this node is currently holding an election. */
    private boolean holdingElection=false;
    /** Map of other node IDs to addresses. */
    private final Map<Integer,InetAddress> otherNodes;
    /** ID of the local node. */
    private final int selfID;
    /** Supplier that returns the last Timestamp that was delivered locally. */
    private final Supplier<Timestamp> getLastTimestampDeliveredLocally;
    /** Supplier that returns an up to date history of delivered messages. */
    private final Supplier<TreeMap<Timestamp,String[]>> getLocalHisTree;
    /** Socket being kept for pinging purposes. */
    private SocketInOutTriple pingLeader;

    /**
     * Create an ElectionManager that follows the Bully Algorithm approximately.
     * @param otherNodes A map of other node IDs to InetAddresses.
     * @param selfID The ID of hte local node.
     * @param getLastTimestampDeliveredLocally A supplier that gives the most recent timestamp that was delivered locally.
     * @param isDone A supplier that returns true when the owner of this BullyManager is done.
     * @param getLocalHisTree A supplier that gives a TreeMap representing Timestamps and their messages.
     */
    public BullyManager(Map<Integer,InetAddress> otherNodes, int selfID,
                        Supplier<Timestamp> getLastTimestampDeliveredLocally, BooleanSupplier isDone,
                        Supplier<TreeMap<Timestamp,String[]>> getLocalHisTree){
      this.otherNodes=otherNodes;
      this.selfID=selfID;
      this.getLastTimestampDeliveredLocally=getLastTimestampDeliveredLocally;
      this.getLocalHisTree=getLocalHisTree;
      pool.execute(()->{
        try(ServerSocket server=new ServerSocket(ELECTION_PORT)) {
          while (!isDone.getAsBoolean()) {
            new ConsumerBasedSocketInOutTriple(
              server.accept(),
              (c,message)->{
                switch (message[0]) {
                  case LATEST_DELIVERED_TIMESTAMP_REQUEST: {
                    //It's asking for the latest timestamp to determine possible leaders, so dovetail into leading
                    c.blockingSendObject(getLastTimestampDeliveredLocally.get());
                    c.blockingRecvMessage();
                    c.blockingSendMessage(OK);
                    if (!holdingElection) {
                      if (initiateElection()) {
                        c.blockingSendObject(leader);
                      }
                    }
                    break;
                  }case PING:{
                    //Ping back. Harder. Ping while we're still the leader.
                    while(leader==selfID){
                      c.blockingSendMessage(PING);
                      if(leader!=selfID) break;
                      c.blockingRecvMessage();
                    }
                    break;
                  }case GET_LEADER_HISTORY_TREE:{
                    //Send over the entire history of commands one a time.
                    TreeMap<Timestamp, String[]> timestampTreeMap = getLocalHisTree.get();
                    timestampTreeMap.forEach((k,v)->{
                      try {
                        c.blockingSendMessage(NEXT);
                        c.blockingSendObject(k);
                        c.blockingSendMessage(v);
                      } catch (IOException ignored) {}
                    });
                    c.blockingSendMessage(END);
                    break;
                  }
                }
                c.close();
              },
              Throwable::printStackTrace);
          }
        } catch (IOException e) {
          e.printStackTrace();
          System.exit(-1);
        }
      });
      initiateElection();
    }

    /**
     * IT'S TIME TO MAKE AMERICA GREAT AGAIN. WE'VE GOT THE BEST STREAMS, OUR STREAMS ARE THE BEST STREAMS. AND FLATMAPS
     * I'M TELLING YOU, BEFORE WE DIDN'T HAVE VERY GOOD FLATMAPS, BUT WE'LL HAVE THE BEST IN THE WORLD.
     * @return FALSE AND THE ELECTION WAS RIGGED AND I'M GONNA KEEP YOU IN SUSPENSE, TRUE IF WE'RE GONNA DRAIN THE SWAMP.
     */
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

    /**
     * Ehem. Anyway.
     * Get the ID of the current leader. If this node is the leader good, otherwise ping the leader, if the ping fails,
     * start an election, then recurse with the new leader.
     *
     * @return The ID of the current leader.
     */
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


    /**
     * Get the history of the leader's delivered messages. If this node is the leader good, otherwise ping the leader,
     * if the ping fails, start an election, then recurse with the new leader.
     *
     * @return The history of the leader's delivered messages as a tree.
     */
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
