import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A node in the ZooKeeper Algorithm.
 */
@SuppressWarnings({"WeakerAccess"})
public class ZooKeeperNode implements AutoCloseable {
  private class TimeStamp implements Serializable, Comparable<TimeStamp> {
    final int epoch;
    final int counter;

    private TimeStamp(int epoch, int counter) {
      this.epoch = epoch;
      this.counter = counter;
    }

    @Override
    public int compareTo(@NotNull TimeStamp o) {
      if (epoch == o.epoch) {
        return Integer.compare(counter, o.counter);
      }
      return Integer.compare(epoch, o.epoch);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TimeStamp timeStamp = (TimeStamp) o;
      return epoch == timeStamp.epoch && counter == timeStamp.counter;
    }

    @Override
    public int hashCode() {
      return 31 * epoch + counter;
    }
  }

  /**
   * Boolean that is true while this node in a ZooKeeperNode algorithm has not crashed.
   */
  private boolean running = true;
  /**
   * Pool of executors that all local ZooKeeperNodes use to preform jobs.
   */
  private static final ExecutorService pool = Executors.newCachedThreadPool();
  /**
   * Port that all ZooKeeperNodes use for setup communications.
   */
  public static final int INTER_NODE_SETUP_PORT = HW2Console.PORT + 255;
  /** Port that all ZooKeeperNodes use for runtime communications. */
  public static final int INTER_NODE_COMM_PORT = INTER_NODE_SETUP_PORT + 255;
  /**
   * String sent from the console or read from the log that says to create a file.
   */
  public static final String CREATE_FILE_COMMAND = "CREATE";
  /**
   * String sent from the console or read from the log that says to create a file.
   */
  public static final String DELETE_FILE_COMMAND = "DELETE";
  /** String sent from the console that says to create a file. */
  public static final String SERVER_READ_FILE_MESSAGE = "READ";
  /**
   * String sent from the console or read from the log that says to create a file.
   */
  public static final String APPEND_FILE_COMMAND = "APPEND_FILE_COMMAND";
  /** String sent from the console that says to kill this node. */
  public static final String SERVER_END_ZOOKEEPER = "EXIT";
  /**
   * String sent from the leader indicating that a broadcast is about to be received.
   */
  public static final String PROPOSE = "PROPOSE";
  /**
   * String sent from a follower to the reader indicating that a proposal has bee been received.
   */
  public static final String ACK = "ACK";
  /** String sent from one ZooKeeperNode to another indicating that a vote reached an abort consensus. */
  public static final String ABORT = "ABORT";
  /** String sent from one ZooKeeperNode to another indicating that a vote reached an commit consensus. */
  public static final String COMMIT = "COMMIT";
  /** Map containing all the current contents of all tokens. */
  public final HashMap<String, String> tokenContents = new HashMap<>();
  /** The list of all otherNodeAddresses that were open last time a message was sent out. */
  public final List<AsyncSocketInOutTriple> consoleConnections = Collections.synchronizedList(new ArrayList<>());
  /** The identification of this ZooKeeperNode. */
  private final int ID;
  /**
   * The address that this ZooKeeperNode can be found at.
   */
  private final InetAddress selfAddress;
  /** Queue of messages to be sent to the connected consoles. */
  private final LinkedTransferQueue<String> consoleMessageQueue = new LinkedTransferQueue<>();
  /**
   * Map to the addresses of every other ZooKeeperNode.
   */
  private final HashMap<Integer, InetAddress> otherNodeAddresses = new HashMap<>();
  /**
   * Thread that sets up a ServerSocket accepting messages from the consoles and that starts processing them.
   */
  private final Runnable incomingConsoleMessageThread = () -> {
    try (ServerSocket consoleServer = new ServerSocket(HW2Console.PORT)) {
      while (running) {
        AsyncSocketInOutTriple in = new AsyncSocketInOutTriple(consoleServer.accept(), (message) -> {
          if (message == null || message.length == 0) return;
          if (message.length == 1 && (message[0].equals(SERVER_END_ZOOKEEPER))) {
            try { close(); } catch (Exception ignored) {}
          }
          switch (message[0]) {
            case CREATE_FILE_COMMAND:
              create(message[1]);
              break;
            case DELETE_FILE_COMMAND:
              delete(message[1]);
              break;
            case SERVER_READ_FILE_MESSAGE:
              read(message[1]);
              break;
            case APPEND_FILE_COMMAND:
              append(message[1], String.join(" ", message).substring(message[0].length() + message[1].length() + 2));
              break;
          }
        });
        consoleConnections.add(in);

      }
    } catch (IOException e) {
      handleConsoleOutput(e, true);
    }
  };
  /**
   * Thread that sets up a ServerSocket accepting messages other ZooKeeperNodes that are starting up.
   */
  private final Runnable incomingStartupMessageThread = () -> {
    while (running) {
      try {
        String message = consoleMessageQueue.take();
        synchronized (consoleConnections) {
          for (int i = 0; i < consoleConnections.size(); i++) {
            AsyncSocketInOutTriple connection = consoleConnections.get(i);
            if (connection == null || !connection.isOpen()) {
              consoleConnections.remove(i);
            } else {
              connection.insertMessage(message);
            }
          }
        }
        System.out.println(message);
      } catch (InterruptedException ignored) {}
    }
  };
  /**
   * Thread that accepts messages from foreign ZooKeeperNodes.
   */
  private final Runnable interNodeCommunicationAcceptor = () -> {
    try (ServerSocket server = new ServerSocket(INTER_NODE_COMM_PORT)) {
      while (running) {
        try (SocketInOutTriple connection = new SocketInOutTriple(server.accept())) {
          int otherNodeID = Integer.parseInt(connection.blockingRecvMessage()[0]);
          String[] command = connection.blockingRecvMessage();
          switch (command[0]) {
            case PROPOSE: {
              followerBroadcastResponse(connection);
              break;
            }
            default: {
              handleConsoleOutput(new RuntimeException("UNRECOGNIZED COMMAND FROM NODE " + otherNodeID + " \"" + Arrays.toString(command) + "\n"), true);
            }
          }
        }
      }
    } catch (ClassNotFoundException | IOException e) {
      handleConsoleOutput(e, true);
    }
  };

  /**
   * The writer that goes to the log.
   */
  private final BufferedWriter logWriter;

  private int newestDeliveredEpoch = 0;
  private int newestDeliveredCounter = 0;
  private int currentEpoch = 0;
  private int currentCounter = 0;

  /**
   * Create a ZooKeeperNode on this computer which will run until it's told to crash.
   *
   * @param ipAddresses The file containing a list of all the ipAddresses of all the ZooKeeperNodes.
   * @param log         The Log file which contains all committed changes. Is executed at startup.
   * @throws IOException Thrown if there is some error
   */
  public ZooKeeperNode(File ipAddresses, File log) throws IOException {
    //Begin accepting commands from consoles.
    pool.execute(incomingConsoleMessageThread);
    //Update this Node's state from the log file.
    updateFromLog(log);
    logWriter = new BufferedWriter(new FileWriter(log, true));
    //Begin accepting commands from starting up ZooKeeperNodes.
    pool.execute(incomingStartupMessageThread);
    //Read each line from the ipAddresses file.
    BufferedReader addressReader = new BufferedReader(new FileReader(ipAddresses));
    String line = addressReader.readLine();
    //The first line in the addressFile is the ID of this Node.
    ID = Integer.parseInt(line.trim());
    handleConsoleOutput("Started node #" + ID);
    //Collect a list of all the InetAddresses of every ZooKeeperNode.
    ArrayList<InetAddress> addressList = new ArrayList<>();
    while ((line = addressReader.readLine()) != null && !(line = line.trim()).equals("")) {
      addressList.add(InetAddress.getByName(line));
    }
    selfAddress = addressList.get(ID);
    handleConsoleOutput("Successfully read file \"" + ipAddresses.getName() + "\"");
    //Receive introductions to each ZooKeeperNode.
    pool.execute(interNodeCommunicationAcceptor);
    pool.execute(() -> {
      try {
        try (ServerSocket server = new ServerSocket(INTER_NODE_SETUP_PORT)) {
          while (otherNodeAddresses.size() < addressList.size()) {
            try (SocketInOutTriple tmp = new SocketInOutTriple(server.accept())) {
              int otherNodeID = (int) tmp.in.readObject();
              InetAddress nodeAddress = (InetAddress) tmp.in.readObject();
              otherNodeAddresses.put(otherNodeID, nodeAddress);
              handleConsoleOutput("Node #" + ID + " received the first connection from Node #" + otherNodeID);
            } catch (ClassNotFoundException e) {
              e.printStackTrace();
            }
          }
          //We send introductions to ourselves, but we don't care.
          otherNodeAddresses.remove(ID);
        }
      } catch (IOException e) {
        handleConsoleOutput(e, true);
      }
    });
    //Send introductions to each ZooKeeperNode.
    addressList.forEach(a -> {
      try (SocketInOutTriple connection = new SocketInOutTriple(new Socket(a, INTER_NODE_SETUP_PORT))) {
        connection.out.writeObject(ID);
        connection.out.writeObject(selfAddress);
        connection.out.flush();
      } catch (IOException e) {
        handleConsoleOutput(e, true);
      }
    });
    //Hang construction until we've received all connections from the other ZooKeeperNodes.
    while (otherNodeAddresses.size() < addressList.size() - 1) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {}
    }
  }

  /**
   * Given a log file, accept without question each command in the log.
   *
   * @param log The File being read from.
   * @throws IOException Thrown if there is a problem reading the log.
   */
  private void updateFromLog(File log) throws IOException {
    //If the log already exists.
    if (!log.createNewFile()) {
      handleConsoleOutput("Beginning to read pre-existing log file.");
      try (BufferedReader logStream = new BufferedReader(new FileReader(log))) {
        String line;
        while ((line = logStream.readLine()) != null && !(line = line.trim()).equals("")) {
          int spaceIndex = line.indexOf(' ');
          if (spaceIndex < 0) {
            handleConsoleOutput(new RuntimeException("THERE WAS AN ERROR IN THE PREVIOUSLY EXISTING LOG FILE'S DATA, LINE WITHOUT SPACE."), true);
          }
          String firstWord = line.substring(0, spaceIndex);
          switch (firstWord) {
            case CREATE_FILE_COMMAND: {
              handleConsoleOutput("Running create command from log file.");
              create(line.substring(spaceIndex + 1));
              break;
            }
            case DELETE_FILE_COMMAND: {
              handleConsoleOutput("Running delete command from log file.");
              delete(line.substring(spaceIndex + 1));
              break;
            }
            case APPEND_FILE_COMMAND: {
              handleConsoleOutput("Running append command from log file.");
              int secondSpaseIndex = line.indexOf(' ', spaceIndex + 1);
              append(line.substring(spaceIndex + 1, secondSpaseIndex), line.substring(secondSpaseIndex + 1));
              break;
            }
            default: {
              handleConsoleOutput(new RuntimeException("THERE WAS AN ERROR IN THE PREVIOUSLY EXISTING LOG FILE'S DATA, UNRECOGNIZED COMMAND."), true);
              break;
            }
          }
        }
      }
      handleConsoleOutput("Finished to reading pre-existing log file.");
    }
  }

  /**
   * When an error is being output print the stack trace to System.err, to each console, and if shouldDie is true exit this
   * JVM.
   *
   * @param e         The error being output.
   * @param shouldDie Whether this is a fatal error.
   */
  public void handleConsoleOutput(Exception e, boolean shouldDie) {
    StringWriter tmp = new StringWriter();
    e.printStackTrace(new PrintWriter(tmp));
    consoleMessageQueue.add(tmp.toString());
    if (!shouldDie) return;
    for (int i = 0; i < 10; i++) {
      if (consoleMessageQueue.isEmpty()) System.exit(-1);
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {}
    }
    System.exit(-1);
  }

  /**
   * When an message should be output put it on the consoleMessageQueue and let the thread for that handle it.
   *
   * @param message The message being output.
   */
  public void handleConsoleOutput(String message) {
    consoleMessageQueue.add(message);
  }

  /**
   * Leader puts forth the given message as a command to all users.
   *
   * @param message The message being sent out for approval.
   * @throws IOException If everything worked out, commit was chosen, but writing to log broke.
   */
  private synchronized void leaderBroadcast(String message[]) throws IOException {
    List<SocketInOutTriple> sockets = otherNodeAddresses.values().parallelStream().
      flatMap(a -> {
        try {
          SocketInOutTriple out = new SocketInOutTriple(new Socket(a, INTER_NODE_COMM_PORT));
          return Stream.of(out);
        } catch (IOException e) {
          return Stream.empty();
        }
      }).
      filter(s -> {
        try {
          s.blockingSendMessage(ID + "");
          s.blockingSendMessage(PROPOSE);
          s.out.writeObject(currentEpoch);
          s.out.writeObject(++currentCounter);
          s.blockingSendMessage(message);
          return true;
        } catch (IOException e) {
          return false;
        }
      }).collect(Collectors.toList());
    append(message[1], String.join(" ", message).substring(message[0].length() + message[1].length() + 2));
    final int[] quorum = {0};
    StampedLock quorumLock = new StampedLock();
    sockets.forEach(s -> pool.execute(() -> {
      try {
        s.blockingRecvMessage();
        if (quorum[0] < (otherNodeAddresses.size() - 1) / 2) {
          long lock = quorumLock.writeLock();
          ++quorum[0];
          quorumLock.unlockWrite(lock);
        }
      } catch (IOException | ClassNotFoundException ignored) {}
    }));
    while (quorum[0] < sockets.size() / 2) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {}
    }
    sockets.forEach(s -> {
      try {
        s.blockingSendMessage(COMMIT);
      } catch (IOException ignored) {}
    });
    while (newestDeliveredEpoch < currentEpoch - 1 || newestDeliveredCounter < currentCounter - 1) {
      try {Thread.sleep(100);} catch (InterruptedException ignored) {}
    }
    newestDeliveredEpoch = currentEpoch;
    newestDeliveredCounter = currentCounter;
    switch (message[0]) {
      case CREATE_FILE_COMMAND:
        create(message[1]);
        break;
      case APPEND_FILE_COMMAND:
        append(message[1], String.join(" ", message).substring(message[0].length() + message[1].length() + 2));
        break;
      case DELETE_FILE_COMMAND:
        delete(message[1]);
        break;
    }
  }

  /**
   * As a non-leader consider two-phase-commit. If the vote commits, append to the log, and alter the memory.
   *
   * @param connection The connection to talk back to the leader.
   */
  private synchronized void followerBroadcastResponse(SocketInOutTriple connection) throws IOException, ClassNotFoundException {
    int receivedEpoch = (int) connection.in.readObject();
    int receivedCounter = (int) connection.in.readObject();
    String[] message = connection.blockingRecvMessage();
    logWriter.write(String.join(" ", message) + "\n");
    connection.blockingSendMessage(ACK);
    connection.blockingRecvMessage();
    while (newestDeliveredEpoch < receivedEpoch - 1 || newestDeliveredCounter < receivedCounter - 1) {
      try {Thread.sleep(100);} catch (InterruptedException ignored) {}
    }
    newestDeliveredEpoch = receivedEpoch;
    newestDeliveredCounter = receivedCounter;
    switch (message[0]) {
      case CREATE_FILE_COMMAND:
        create(message[1]);
        break;
      case APPEND_FILE_COMMAND:
        append(message[1], String.join(" ", message).substring(message[0].length() + message[1].length() + 2));
        break;
      case DELETE_FILE_COMMAND:
        delete(message[1]);
        break;
    }
  }

  /**
   * Actually read the given token, send it to all consoles.
   * @param tokenName The name of the token to read.
   */
  private synchronized void read(String tokenName) {
    handleConsoleOutput("File \"" + tokenName + "\':\n" + tokenContents.get(tokenName) + "End of File \"" + tokenName + "\"");
  }

  /**
   * Actually delete the given token.
   * @param tokenName The name of the token to delete.
   */
  private synchronized void delete(String tokenName) {
    tokenContents.remove(tokenName);
  }

  /**
   * Actually create the given token.
   * @param tokenName The name of the token to create.
   */
  private synchronized void create(String tokenName) {
    tokenContents.put(tokenName, "");
  }

  /**
   * Actually append to the given token.
   *
   * @param tokenName The name of the token to append to.
   * @param line      The line that gets appended.
   */
  private synchronized void append(String tokenName, String line) {
    tokenContents.compute(tokenName, (k, v) -> v + line);
  }


  /**
   * The ip addresses file should be first, and should contain the ID of this node, and then on each line the IP
   * addresses of each file, their line being their ID+1.
   * @param args Hopefully the file of ip addresses, followed by the log.
   */
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("usage: ZooKeeperNode.jar ip_addresses.txt log.txt");
    }
    //Repeatedly create new Zookeepers.
    while (true) {
      //Create new zookeeper, if there is no ip addresses file exit yelling at the user.
      try (ZooKeeperNode keeper = new ZooKeeperNode(new File(args[0]), new File(args[0]))) {
        //Take half second sleeps until the zookeeper dies.
        while (keeper.running) {
          try {Thread.sleep(500);} catch (InterruptedException ignored) {}
        }
      } catch (IOException e) {
        e.printStackTrace();
        System.err.println("THERE WAS AN ERROR READING THE FILE CONTAINING ALL THE IP ADDRESSES.");
        break;
      }
    }
    System.exit(-1);
  }


  /**
   * Close every connection to consoles. Stop running.
   */
  @Override
  public void close() {
    running = false;
    consoleConnections.forEach(c -> {if (c.isOpen()) c.close();});
  }
}
