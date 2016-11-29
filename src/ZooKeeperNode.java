import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;

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
  /** The writer that goes to the log. */
  private final BufferedWriter logWriter;
  /** The Timestamp of the most recent delivered command. */
  private Timestamp newestDeliveredTimestamp = new Timestamp(0, 0);
  /** The Timestamp of the current command. Incremented when a leader sends out the call or the call is received. */
  private Timestamp currentTimestamp = new Timestamp(0, 0);
  /** Thread that sets up a ServerSocket accepting messages from the consoles and that starts processing them. */
  private final ServerSocket incomingConsoleServer;
  private ServerSocket getIncomingConsoleServer(){ return incomingConsoleServer;}
  private final Runnable incomingConsoleMessageThread = () -> {
    while (running) {
      try {
        ConsumerBasedSocketInOutTriple in = new ConsumerBasedSocketInOutTriple(getIncomingConsoleServer().accept(), (c, message) -> {
          if (message == null || message.length == 0) return;
          handleConsoleOutput("Received a message from a console: " + Arrays.toString(message));
          if (message.length == 1 && (message[0].equals(SERVER_END_ZOOKEEPER))) {
            handleConsoleOutput("Received a exit message, dieing now.");
            try { close(); } catch (Exception ignored) {}
          } else if (message[0].equals(SERVER_READ_FILE_MESSAGE)) {
            read(message[1]);
          } else if (message.equals(SERVER_END_ZOOKEEPER)) {
            close();
          } else if (getElectionManager().isLeader()) {
            leaderBroadcast(message);
          } else {
            int leaderID = getElectionManager().getLeaderID();
            if (leaderID == getID()) {
              currentTimestamp = currentTimestamp.incrementEpoch();
              leaderBroadcast(message);
            } else {
              try (SocketInOutTriple connection = new SocketInOutTriple(new Socket(otherNodeAddresses.get(leaderID), INTER_NODE_COMM_PORT))) {
                connection.blockingSendMessage(message);
              }
            }
          }
        }, (e) -> handleConsoleOutput("Connection to a console closed."));
        handleConsoleOutput("Received a connection from a console.");
        consoleConnections.add(in);
      } catch (IOException e) {
        handleConsoleOutput("THERE WAS AN ERROR ACCEPTING A CONNECTION FROM THE CONSOLE.");
      }
    }
  };
  /** Thread sends messages in the queue for the consoles to each console and prints it. */
  private final Runnable outgoingConsoleMessageQueue = () -> {
    while (running) {
      try {
        String message = consoleMessageQueue.take();
        synchronized (consoleConnections) {
          for (int i = 0; i < consoleConnections.size(); i++) {
            ConsumerBasedSocketInOutTriple connection = consoleConnections.get(i);
            if (connection == null || !connection.isOpen()) {
              consoleConnections.remove(i);
              --i;
            } else {
              connection.sendMessage(message);
            }
          }
        }
        System.out.println(message);
      } catch (InterruptedException ignored) {}
    }
  };
  private final ServerSocket interNodeCommunicationServer;
  private ServerSocket getInterNodeCommunicationServer(){return interNodeCommunicationServer;}
  /** Thread that accepts messages from foreign ZooKeeperNodes. */
  private final Runnable interNodeCommunicationAcceptor = () -> {
    while (running) {
      try {
        new ConsumerBasedSocketInOutTriple(
          getInterNodeCommunicationServer().accept(),
          (c,message)-> {
            handleConsoleOutput("Received a message from another ZooKeeper: "+Arrays.toString(message));
            switch (message[0]) {
              case PROPOSE: {
                followerBroadcastResponse(c);
                break;
              }
              case CREATE_FILE_COMMAND:
              case DELETE_FILE_COMMAND:
              case APPEND_FILE_COMMAND: {
                int leaderID = getElectionManager().getLeaderID();
                if(leaderID== getID()) {
                  leaderBroadcast(message);
                }else{
                  try(SocketInOutTriple connection=new SocketInOutTriple(new Socket(otherNodeAddresses.get(leaderID),INTER_NODE_COMM_PORT))){
                    connection.blockingSendMessage(message);
                  }
                }
                break;
              }
            }
            c.close();
          },
          (e)->handleConsoleOutput("THERE WAS AN ERROR ACCEPTING COMMANDS FROM ANOTHER ZOOKEEPER."));
      } catch (IOException ignored) {}
    }
  };
  /** A map of the most recent Timestamp seen within each epoch, given that they'll have different lengths and we need
   * to block based on the most recently delivered one. */
  private HashMap<Integer, Timestamp> newestTimestampsInEpoch = new HashMap<>();
  /** The ElectionManager that handles all election mandated communication */
  private final ElectionManager electionManager;
  /** The sorted Map representing all the history of delivered commands. */
  private final TreeMap<Timestamp, String[]> localHisTree;

  /**
   * Create a ZooKeeperNode on this computer which will run until it's told to crash.
   *
   * @param ipAddresses The file containing a list of all the ipAddresses of all the ZooKeeperNodes.
   * @param log         The Log file which contains all committed changes. Is executed at startup.
   * @throws IOException Thrown if there is some error
   */
  public ZooKeeperNode(String ipAddresses, String log) throws IOException {
    interNodeCommunicationServer=new ServerSocket(INTER_NODE_COMM_PORT);
    incomingConsoleServer = new ServerSocket(HW2Console.PORT);
    newestTimestampsInEpoch.put(0,currentTimestamp);
    //Begin handling output.
    pool.execute(outgoingConsoleMessageQueue);
    //Read each line from the ipAddresses file.
    InetAddress tmp = null;
    try(FileReader fReader=new FileReader(new File(ipAddresses));
      BufferedReader addressReader = new BufferedReader(fReader)) {
      String line = addressReader.readLine();
      //The first line in the addressFile is the ID of this Node.
      ID = Integer.parseInt(line.trim());
      handleConsoleOutput("Started node #" + ID);
      //Collect a list of all the InetAddresses of every ZooKeeperNode.
      int ctr = 0;
      while ((line = addressReader.readLine()) != null && !(line = line.trim()).equals("")) {
        if (ctr == ID) {
          ++ctr;
          tmp = InetAddress.getByName(line);
        } else {
          otherNodeAddresses.put(ctr++, InetAddress.getByName(line));
        }
      }
    }
    selfAddress = tmp;
    if (selfAddress == null) handleConsoleOutput(new RuntimeException("THIS NODE NEVER READ IT'S OWN ADDRESS."), true);
    handleConsoleOutput("Successfully read file \"" + ipAddresses + "\"");
    //Start up the election manager.
    electionManager = new ElectionManager.BullyManager(otherNodeAddresses, ID,
      this::getNewestDeliveredTimestamp, ()->!this.isRunning(),
      this::getLocalHisTree,this::handleConsoleOutput);
    //Find out who the leader is
    int leader = electionManager.getLeaderID();
    //HisTree, get it? It's a pun!... yea maybe that's better in my head, but we're sticking with it.
    //If we are the leader read our own log for history.
    if (leader == ID) {
      File f=new File(log);
      localHisTree = readOwnHisTree(f);
      executeHisTree(localHisTree);
      setCurrentTimestamp(currentTimestamp.incrementEpoch());
      logWriter = new BufferedWriter(new FileWriter(log, true));
    } else {
      //Otherwise get the history of the leader and do it.
      localHisTree=electionManager.getLeaderHisTree();
      logWriter = new BufferedWriter(new FileWriter(log, false));
      localHisTree.forEach((k, v) -> {
        try {
          logWriter.write(k.toString() + " " + String.join(" ", v) + "\n");
        } catch (IOException e) {
          handleConsoleOutput("THERE WAS AN ERROR WRITING TO THE LOG, THIS ZOOKEEPER IS COMMITTING SODUKU");
          close();
        }
      });
      logWriter.flush();
      executeHisTree(localHisTree);
    }
    //Begin accepting commands from consoles.
    pool.execute(incomingConsoleMessageThread);
    //Receive messages from each zookeeper.
    pool.execute(interNodeCommunicationAcceptor);
  }

  /**
   * Given a log file, read and assemble a tree of commands.
   *
   * @param log The File being read from.
   * @return The TreeMap of timestamps to messages.
   * @throws IOException Thrown if there is a problem reading the log.
   */
  private TreeMap<Timestamp, String[]> readOwnHisTree(File log) throws IOException {
    //If the log already exists.
    if (!log.createNewFile()) {
      handleConsoleOutput("Beginning to read pre-existing log file.");
      TreeMap<Timestamp, String[]> hisTree = new TreeMap<>();
      try (FileReader fReader=new FileReader(log);
        BufferedReader logStream = new BufferedReader(fReader)) {
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
    TreeMap<Timestamp,String[]> copy=new TreeMap<>(hisTree);
    while (!copy.isEmpty()) {
      Timestamp nextTime = copy.firstKey();
      setCurrentTimestamp(nextTime);
      newestDeliveredTimestamp = nextTime;
      String[] nextCommand = copy.pollFirstEntry().getValue();
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
    }
    handleConsoleOutput("Finished executing a hisTree, now know about tokens: "+tokenContents.keySet().stream().reduce((a,b)->a+" "+b).orElse(""));
  }

  /**
   * When an error is being output print the stack trace to System.err, to each console, and if shouldDie is true exit this
   * JVM.
   *
   * @param e         The error being output.
   * @param shouldDie Whether this is a fatal error.
   */
  public void handleConsoleOutput(Throwable e, boolean shouldDie) {
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
    handleConsoleOutput("As leader, began broadcasting ["+Arrays.toString(message)+"]");
    final int[] quorum = {1};
    logWriter.write(currentTimestamp.toString() + " " + String.join(" ", message) + "\n");
    logWriter.flush();
    //Spin up threads to repeatedly attempt to communicate with each follower
    //Thought about using parallel stream but that has a hard limit on the threadpool which could cause problems
    otherNodeAddresses.values().forEach(a -> pool.execute(() -> {
      while (running) {
        try (SocketInOutTriple connection = new SocketInOutTriple(new Socket(a, INTER_NODE_COMM_PORT))) {
          connection.blockingSendMessage(PROPOSE,ID+"", "["+String.join(" ",message)+"]");
          connection.blockingSendObject(currentTimestamp);
          connection.blockingSendMessage(message);
          connection.blockingRecvMessage();
          synchronized (quorum) {
            quorum[0]+=1;
          }
          try{
            while (running) {
              synchronized (quorum) {
                if (quorum[0] > (otherNodeAddresses.size() + 1) / 2) {
                  connection.blockingSendMessage(COMMIT);
                  return;
                }
              }
              safeSleep(100);
            }
          }catch (IOException ext){
            quorum[0]-=1;
          }
        } catch (IOException | ClassNotFoundException ignored) {
          safeSleep(500);
        }
      }
    }));
    //Block this thread while we wait for the quorum to be achieved
    handleConsoleOutput("Waiting for a quorum.");
    while (running) {
      synchronized (quorum) {
        if (quorum[0] > (otherNodeAddresses.size() + 1) / 2) {
          handleConsoleOutput("Got a quorum.");
          break;
        }
      }
      safeSleep(500);
    }
    handleConsoleOutput("Waiting for previous messages to be delivered.");
    while (!(currentTimestamp.epoch == newestDeliveredTimestamp.epoch&&currentTimestamp.counter-1==newestDeliveredTimestamp.counter) &&
      !(currentTimestamp.epoch - 1 == newestDeliveredTimestamp.epoch && currentTimestamp.counter == 1
        && newestDeliveredTimestamp.counter == newestTimestampsInEpoch.getOrDefault(newestDeliveredTimestamp.epoch, newestDeliveredTimestamp).counter)) {
      safeSleep(100);
    }
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
    handleConsoleOutput("As a leader, delivered ["+Arrays.toString(message)+"]");
    newestDeliveredTimestamp = currentTimestamp;
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
    handleConsoleOutput("As as follower, began receiving ["+Arrays.toString(message)+"]");
    logWriter.write(receivedTimestamp.toString() + " " + String.join(" ", message) + "\n");
    connection.blockingSendMessage(ACK);
    handleConsoleOutput("Waiting for a quorum.");
    connection.blockingRecvMessage();
    while (!(currentTimestamp.epoch == newestDeliveredTimestamp.epoch&&currentTimestamp.counter-1==newestDeliveredTimestamp.counter) &&
      !(currentTimestamp.epoch - 1 == newestDeliveredTimestamp.epoch && currentTimestamp.counter == 1
        && newestDeliveredTimestamp.counter == newestTimestampsInEpoch.getOrDefault(newestDeliveredTimestamp.epoch, newestDeliveredTimestamp).counter)) {
      safeSleep(100);
    }
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
    handleConsoleOutput("As a follower, delivered ["+Arrays.toString(message)+"]");
    newestDeliveredTimestamp = receivedTimestamp;
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
    String tokenContent=tokenContents.get(tokenName);
    if(tokenContent==null){
      handleConsoleOutput("At time ("+newestDeliveredTimestamp+") attempted to read file \""+tokenName+"\" which does not currently exist.");
    }else {
      handleConsoleOutput("At time ("+newestDeliveredTimestamp+") file \"" + tokenName + "\':\n" + tokenContents.get(tokenName) + "\nEnd of File \"" + tokenName + "\"");
    }
  }

  /**
   * Actually delete the given token.
   * @param tokenName The name of the token to delete.
   */
  private synchronized void delete(String tokenName) {
    if(tokenContents.remove(tokenName)==null){
      handleConsoleOutput("At time ("+newestDeliveredTimestamp+") attempted to read file \""+tokenName+"\" which does not currently exist.");
    }else{
      handleConsoleOutput("At time ("+newestDeliveredTimestamp+") deleted file \"" + tokenName + "\"");
    }
  }

  /**
   * Actually create the given token.
   * @param tokenName The name of the token to create.
   */
  private synchronized void create(String tokenName) {
    if(tokenContents.containsKey(tokenName)){
      handleConsoleOutput("At time ("+newestDeliveredTimestamp+") attempted to create file \""+tokenName+"\" which already exists.");
    }else {
      tokenContents.put(tokenName, "");
      handleConsoleOutput("At time ("+newestDeliveredTimestamp+") created file \"" + tokenName + "\"");
    }
  }

  /**
   * Actually append to the given token.
   *
   * @param tokenName The name of the token to append to.
   * @param line      The line that gets appended.
   */
  private synchronized void append(String tokenName, String line) {
    if(tokenContents.containsKey(tokenName)){
      handleConsoleOutput("At time ("+newestDeliveredTimestamp+") attempted to read file \""+tokenName+"\" which does not currently exist.");
    }else {
      tokenContents.compute(tokenName, (k, v) -> v + line+"\n");
      handleConsoleOutput("At time ("+newestDeliveredTimestamp+") appended \""+line+"\" to file \""+tokenName+"\"");
    }
  }


  /**
   * The ip addresses file should be first, and should contain the ID of this node, and then on each line the IP
   * addresses of each file, their line being their ID+1.
   * @param args Hopefully the file of ip addresses, followed by the log.
   */
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("usage: ZooKeeperNode.jar ip_addresses.txt log.txt");
      System.exit(-1);
    }
    //Repeatedly create new Zookeepers.
    while (true) {
      //Create new zookeeper, if there is no ip addresses file exit yelling at the user.
      try (ZooKeeperNode keeper = new ZooKeeperNode(args[0], args[1])) {
        //Take half second sleeps until the zookeeper dies.
        while (keeper.running) {
          safeSleep(500);
        }
      } catch (IOException e) {
        e.printStackTrace();
        System.err.println("THERE WAS AN ERROR READING THE FILE CONTAINING ALL THE IP ADDRESSES.");
        break;
      }
      safeSleep(1000);
    }
    System.exit(-1);
  }


  /**
   * Close every connection to consoles. Stop running.
   */
  @Override
  public void close() {
    running = false;
    electionManager.close();
    try {logWriter.flush(); } catch (IOException ignored) {}
    try {logWriter.close();} catch (IOException ignored) {}
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

  public ElectionManager getElectionManager() {
    return electionManager;
  }

  public int getID() {
    return ID;
  }
}
