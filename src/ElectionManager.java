import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
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
  /** Return the Epoch */
  int getEpoch();


  class Bully implements ElectionManager{
    private static final String START_ELECTION="START_ELECTION";
    private static final String LATEST_DELIVERED_TIMESTAMP_REQUEST="LATEST_DELIVERED_TIMESTAMP_REQUEST";
    private static final String OK="OK";
    /** Pool of executors that all local Bullies use to preform jobs. */
    private static final ExecutorService pool = Executors.newCachedThreadPool();
    /** Port that all Bullies use for election communication. */
    public static final int ELECTION_PORT = ZooKeeperNode.INTER_NODE_COMM_PORT + 255;
    private int leader=-1;
    private boolean holdingElection=false;
    private final List<InetAddress> otherNodes;
    private final int selfID;
    Supplier<Timestamp> getLastTimestampDelivered;
    public Bully(List<InetAddress> otherNodes, int selfID, Supplier<Timestamp> getLastTimestampDelivered, BooleanSupplier isDone){
      this.otherNodes=otherNodes;
      this.selfID=selfID;
      pool.execute(()->{
        try(ServerSocket server=new ServerSocket(ELECTION_PORT)) {
          while (!isDone.getAsBoolean()) {
            SocketInOutTriple connection=new SocketInOutTriple(server.accept());
            pool.execute(()-> {
              try {
                String message[] = connection.blockingRecvMessage();
                switch (message[0]) {
                  case LATEST_DELIVERED_TIMESTAMP_REQUEST: {
                    connection.out.writeObject(getLastTimestampDelivered.get());
                    connection.out.writeObject(selfID);
                    connection.out.flush();
                    connection.blockingRecvMessage();
                    connection.blockingSendMessage(OK);
                    if (!holdingElection) {
                      if (initiateElection()) {
                        connection.out.writeObject(leader);
                        connection.out.flush();
                      }
                    }
                    break;
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
      Timestamp timestamp = getLastTimestampDelivered.get();
      int coordinator=otherNodes.stream().
        flatMap(a -> {
          if(!holdingElection) return Stream.empty();
          try {
            return Stream.of(new SocketInOutTriple(new Socket(a, ELECTION_PORT)));
          } catch (IOException e) {
            return Stream.empty();
          }
        }).
        flatMap(connection->{
          if(!holdingElection) return Stream.empty();
          try {
            connection.blockingSendMessage(LATEST_DELIVERED_TIMESTAMP_REQUEST);
            if(!holdingElection) return Stream.empty();
            if(timestamp.compareTo((Timestamp)connection.in.readObject())<-1){
              if(selfID<(int)connection.in.readObject()) {
                if (!holdingElection) return Stream.empty();
                return Stream.of(connection);
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
      return leader;
    }

    @Override
    public int getEpoch() {
      return 0;
    }
  }
}
