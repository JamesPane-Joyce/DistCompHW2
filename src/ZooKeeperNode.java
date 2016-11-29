import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.locks.StampedLock;

/**
 * A node in the ZooKeeper Algorithm.
 */
@SuppressWarnings({"WeakerAccess", "FieldCanBeLocal"})
public class ZooKeeperNode implements AutoCloseable {
  /** Boolean that is true while this node in a ZooKeeperNode algorithm has not crashed. */
  private boolean running = true;
  /** Pool of executors that all local ZooKeeperNodes use to preform jobs. */
  private static final ExecutorService pool = Executors.newCachedThreadPool();
  /** Port that all ZooKeeperNodes use for runtime communications. */
  public static final int INTER_NODE_COMM_PORT = HW2Console.PORT + 255;
  /** String sent from the console or read from the log that says to create a file. */
  public static final String CREATE_FILE_COMMAND = "CREATE";
  /** String sent from the console or read from the log that says to create a file. */
  public static final String DELETE_FILE_COMMAND = "DELETE";
  /** String sent from the console that says to create a file. */
  public static final String SERVER_READ_FILE_MESSAGE = "READ";
  /** String sent from the console or read from the log that says to create a file. */
  public static final String APPEND_FILE_COMMAND = "APPEND_FILE_COMMAND";
  /** String sent from the console that says to kill this node. */
  public static final String SERVER_END_ZOOKEEPER = "EXIT";
  /** * String sent from the leader indicating that a broadcast is about to be received. */
  public static final String PROPOSE = "PROPOSE";
  /** String sent from a follower to the reader indicating that a proposal has bee been received. */
  public static final String ACK = "ACK";
  /** String sent from a leader to all followers to another indicating that a proposal has finished. */
  public static final String COMMIT = "COMMIT";
  /** Map containing all the current contents of all tokens. */
  public final HashMap<String, String> tokenContents = new HashMap<>();
  /** The list of all otherNodeAddresses that were open last time a message was sent out. */
  public final List<ConsumerBasedSocketInOutTriple> consoleConnections = Collections.synchronizedList(new ArrayList<>());
  /** The identification of this ZooKeeperNode. */
  private final int ID;
  /** The address that this ZooKeeperNode can be found at. */
  private final InetAddress selfAddress;
  /** Queue of messages to be sent to the connected consoles. */
  private final LinkedTransferQueue<String> consoleMessageQueue = new LinkedTransferQueue<>();
  /** Map to the addresses of every other ZooKeeperNode. */
  private final TreeMap<Integer, InetAddress> otherNodeAddresses = new TreeMap<>();
  /** Thread that sets up a ServerSocket accepting messages from the consoles and that starts processing them. */
  private final Runnable incomingConsoleMessageThread = () -> {
    try (ServerSocket consoleServer = new ServerSocket(HW2Console.PORT)) {
      while (running) {
        ConsumerBasedSocketInOutTriple in = new ConsumerBasedSocketInOutTriple(consoleServer.accept(), (message) -> {
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
  /** Thread that sets up a ServerSocket accepting messages from consoles that are starting up. */
  private final Runnable incomingStartupMessageThread = () -> {
    while (running) {
      try {
        String message = consoleMessageQueue.take();
        synchronized (consoleConnections) {
          for (int i = 0; i < consoleConnections.size(); i++) {
            ConsumerBasedSocketInOutTriple connection = consoleConnections.get(i);
            if (connection == null || !connection.isOpen()) {
              consoleConnections.remove(i);
            } else {
              connection.processMessage(message);
            }
          }
        }
        System.out.println(message);
      } catch (InterruptedException ignored) {}
    }
  };
  /** Thread that accepts messages from foreign ZooKeeperNodes. */
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

  /** The writer that goes to the log. */
  private final BufferedWriter logWriter;

  /** The Timestamp of the most recent delivered command. */
  private Timestamp newestDeliveredTimestamp = new Timestamp(0, -1);
  /** The Timestamp of the current command. Incremented when a leader sends out the call or the call is received. */
  private Timestamp currentTimestamp = new Timestamp(0, -1);
  /** A map of the most recent Timestamp seen within each epoch, given that they'll have different lengths and we need
   * to block based on the most recently delivered one. */
  private HashMap<Integer, Timestamp> newestTimestampsInEpoch = new HashMap<>();
  private final ElectionManager electionManager;
  private final TreeMap<Timestamp, String[]> localHisTree;

  /**
   * Create a ZooKeeperNode on this computer which will run until it's told to crash.
   *
   * @param ipAddresses The file containing a list of all the ipAddresses of all the ZooKeeperNodes.
   * @param log         The Log file which contains all committed changes. Is executed at startup.
   * @throws IOException Thrown if there is some error
   */
  public ZooKeeperNode(File ipAddresses, File log) throws IOException {
    //Read each line from the ipAddresses file.
    BufferedReader addressReader = new BufferedReader(new FileReader(ipAddresses));
    String line = addressReader.readLine();
    //The first line in the addressFile is the ID of this Node.
    ID = Integer.parseInt(line.trim());
    handleConsoleOutput("Started node #" + ID);
    //Collect a list of all the InetAddresses of every ZooKeeperNode.
    int ctr = 0;
    InetAddress tmp = null;
    while ((line = addressReader.readLine()) != null && !(line = line.trim()).equals("")) {
      if (ctr == ID) {
        ++ctr;
        tmp = InetAddress.getByName(line);
        continue;
      }
      otherNodeAddresses.put(ctr++, InetAddress.getByName(line));
    }
    selfAddress = tmp;
    electionManager = new ElectionManager.BullyManager(otherNodeAddresses, ID, this::getNewestDeliveredTimestamp, this::isRunning, this::getLocalHisTree);
    int leader = electionManager.getLeaderID();
    //HisTree, get it? It's a pun!... yea maybe that's better in my head, but we're sticking with it.
    localHisTree = readOwnHisTree(log);
    if (leader == ID || (localHisTree.lastKey() == electionManager.getTimestamp() && electionManager.getLeaderID() == leader)) {
      executeHisTree(localHisTree);
      logWriter = new BufferedWriter(new FileWriter(log, true));
    } else {
      localHisTree.clear();
      localHisTree.putAll(electionManager.getLeaderHisTree());
      logWriter = new BufferedWriter(new FileWriter(log, false));
      localHisTree.forEach((k, v) -> {
        try {
          logWriter.write(k.toString() + " " + String.join(" ", v) + "\n");
        } catch (IOException e) {
          handleConsoleOutput(e, true);
        }
      });
      executeHisTree(localHisTree);
    }
    //Begin accepting commands from consoles.
    pool.execute(incomingConsoleMessageThread);
    //Update this Node's state from the log file.
    //Begin accepting commands from starting up ZooKeeperNodes.
    pool.execute(incomingStartupMessageThread);
    if (selfAddress == null) handleConsoleOutput(new RuntimeException("THIS NODE NEVER READ IT'S OWN ADDRESS."), true);
    handleConsoleOutput("Successfully read file \"" + ipAddresses.getName() + "\"");
    //Receive introductions to each ZooKeeperNode.
    pool.execute(interNodeCommunicationAcceptor);
  }

  /**
   * Given a log file, accept without question each command in the log.
   *
   * @param log The File being read from.
   * @throws IOException Thrown if there is a problem reading the log.
   */
  private TreeMap<Timestamp, String[]> readOwnHisTree(File log) throws IOException {
    //If the log already exists.
    if (!log.createNewFile()) {
      handleConsoleOutput("Beginning to read pre-existing log file.");
      TreeMap<Timestamp, String[]> hisTree = new TreeMap<>();
      try (BufferedReader logStream = new BufferedReader(new FileReader(log))) {
        String line;
        while ((line = logStream.readLine()) != null && !(line = line.trim()).equals("")) {
          int spaceIndex = line.indexOf(' ');
          if (spaceIndex < 0) {
            handleConsoleOutput(new RuntimeException("THERE WAS AN ERROR IN THE PREVIOUSLY EXISTING LOG FILE'S DATA, LINE WITHOUT SPACE."), true);
          }
          int readEpoch = Integer.parseInt(line.substring(0, spaceIndex));
          line = line.substring(spaceIndex + 1);
          spaceIndex = line.indexOf(' ');
          if (spaceIndex < 0) {
            handleConsoleOutput(new RuntimeException("THERE WAS AN ERROR IN THE PREVIOUSLY EXISTING LOG FILE'S DATA, LINE WITHOUT COUNTER."), true);
          }
          int readCounter = Integer.parseInt(line.substring(0, spaceIndex));
          line = line.substring(spaceIndex + 1);
          hisTree.put(new Timestamp(readEpoch, readCounter), line.split(" "));
        }
      }
      handleConsoleOutput("Finished to reading pre-existing log file.");
      return hisTree;
    }
    return new TreeMap<>();
  }

  /**
   * Given an ordered TreeMap of Timestamps to their command Strings, execute the ones with the smallest stamps. Update
   * current Timestamp and the most recent delivered Timestamp.
   *
   * @param hisTree The ordered TreeMap from least timestamp to most recent.
   */
  private void executeHisTree(TreeMap<Timestamp, String[]> hisTree) {
    while (!hisTree.isEmpty()) {
      Timestamp nextTime = hisTree.firstKey();
      setCurrentTimestamp(nextTime);
      String[] nextCommand = hisTree.pollFirstEntry().getValue();
      switch (nextCommand[0]) {
        case CREATE_FILE_COMMAND: {
          create(nextCommand[1]);
          break;
        }
        case APPEND_FILE_COMMAND: {
          append(nextCommand[1], String.join(" ", nextCommand).substring(nextCommand[0].length() + nextCommand[1].length() + 2));
          break;
        }
        case DELETE_FILE_COMMAND: {
          delete(nextCommand[1]);
          break;
        }
      }
      newestDeliveredTimestamp = nextTime;
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
      safeSleep(100);
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
    setCurrentTimestamp(currentTimestamp.nextCounterTimestamp());
    final int[] quorum = {0};
    StampedLock quorumLock = new StampedLock();
    logWriter.write(currentTimestamp.toString() + " " + String.join(" ", message) + "\n");
    //Spin up threads to repeatedly attempt to communicate with each follower
    //Thought about using parallel stream but that has a hard limit on the threadpool which could cause problems
    otherNodeAddresses.values().forEach(a -> pool.execute(() -> {
      while (running) {
        long lock = quorumLock.readLock();
        if (quorum[0] > (otherNodeAddresses.size() + 1) / 2) {
          quorumLock.unlockRead(lock);
          break;
        }
        try (SocketInOutTriple connection = new SocketInOutTriple(new Socket(a, INTER_NODE_COMM_PORT))) {
          connection.blockingSendMessage(ID + "");
          connection.blockingSendMessage(PROPOSE);
          connection.out.writeObject(currentTimestamp);
          connection.blockingSendMessage(message);
          connection.blockingRecvMessage();
          lock = quorumLock.writeLock();
          ++quorum[0];
          quorumLock.unlockWrite(lock);
          while (running) {
            lock = quorumLock.readLock();
            if (quorum[0] > (otherNodeAddresses.size() + 1) / 2) {
              quorumLock.unlockRead(lock);
              break;
            }
          }
          connection.blockingSendMessage(COMMIT);
        } catch (IOException | ClassNotFoundException ignored) {
          safeSleep(500);
          continue;
        }
        break;
      }
    }));
    //Block this thread while we wait for the quorum to be achieved
    while (running) {
      long lock = quorumLock.readLock();
      if (quorum[0] > (otherNodeAddresses.size() + 1) / 2) {
        quorumLock.unlockRead(lock);
        break;
      }
    }
    while (currentTimestamp.epoch > newestDeliveredTimestamp.epoch &&
      !(currentTimestamp.epoch - 1 == newestDeliveredTimestamp.epoch && currentTimestamp.counter == 0
        && newestDeliveredTimestamp.counter == newestTimestampsInEpoch.getOrDefault(newestDeliveredTimestamp.epoch, newestDeliveredTimestamp).counter)) {
      safeSleep(100);
    }
    newestDeliveredTimestamp = currentTimestamp;
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

  private static void safeSleep(int millis) {
    try {Thread.sleep(millis);} catch (InterruptedException ignored) {}
  }

  /**
   * As a non-leader consider two-phase-commit. If the vote commits, append to the log, and alter the memory.
   *
   * @param connection The connection to talk back to the leader.
   */
  private synchronized void followerBroadcastResponse(SocketInOutTriple connection) throws IOException, ClassNotFoundException {
    Timestamp receivedTimestamp = (Timestamp) connection.in.readObject();
    setCurrentTimestamp(receivedTimestamp);

    String[] message = connection.blockingRecvMessage();
    logWriter.write(receivedTimestamp.toString() + " " + String.join(" ", message) + "\n");
    connection.blockingSendMessage(ACK);
    connection.blockingRecvMessage();
    while (receivedTimestamp.epoch > newestDeliveredTimestamp.epoch &&
      !(receivedTimestamp.epoch - 1 == newestDeliveredTimestamp.epoch && receivedTimestamp.counter == 0
        && newestDeliveredTimestamp.counter == newestTimestampsInEpoch.getOrDefault(newestDeliveredTimestamp.epoch, newestDeliveredTimestamp).counter)) {
      safeSleep(100);
    }
    newestDeliveredTimestamp = receivedTimestamp;
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

  private void setCurrentTimestamp(Timestamp timestamp) {
    newestTimestampsInEpoch.compute(timestamp.epoch, (k, v) -> v == null || v.counter < timestamp.counter ? timestamp : v);
    if (timestamp.compareTo(currentTimestamp) > 0) {
      currentTimestamp = timestamp;
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
          safeSleep(500);
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

  public Timestamp getNewestDeliveredTimestamp() {
    return newestDeliveredTimestamp;
  }

  public boolean isRunning() {
    return running;
  }

  public TreeMap<Timestamp,String[]> getLocalHisTree() {
    return localHisTree;
  }
}
