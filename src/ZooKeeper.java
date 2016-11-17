import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by james on 10/27/16.
 */
public class ZooKeeper implements AutoCloseable {
  private boolean running = true;
  private static final ExecutorService pool = Executors.newCachedThreadPool();
  public static final int INTER_NODE_SETUP_PORT = HW2Console.PORT + 255;
  public static final int INTER_NODE_COMM_PORT = INTER_NODE_SETUP_PORT + 255;
  public static final String SERVER_CREATE_FILE_MESSAGE = "CREATE";
  public static final String SERVER_DELETE_FILE_MESSAGE = "DELETE";
  public static final String SERVER_READ_FILE_MESSAGE = "READ";
  public static final String SERVER_APPEND_FILE_MESSAGE = "APPEND";
  public static final String SERVER_END_ZOOKEEPER = "EXIT";
  public static final String VOTE_ABORT = "VOTE_ABORT";
  public static final String VOTE_COMMIT = "VOTE_COMMIT";
  public static final String ABORT = "ABORT";
  public static final String COMMIT = "COMMIT";
  public final HashMap<String, String> tokenContents = new HashMap<>();
  public final StampedLock globalConnectionLock = new StampedLock();
  public final List<AsyncSocketInOutTriple> consoleConnections = Collections.synchronizedList(new ArrayList<>());
  private final int ID;
  private final LinkedTransferQueue<String> consoleMessageQueue = new LinkedTransferQueue<>();
  private final HashMap<String, InetAddress> connections = new HashMap<>();
  private final Consumer<String[]> consoleMessageConsumer = (message) -> {
    if (message == null || message.length == 0) return;
    if (message.length == 1 && (message[0].equals(SERVER_END_ZOOKEEPER))) {
      try { close(); } catch (Exception ignored) {}
    }
    switch (message[0]) {
      case SERVER_CREATE_FILE_MESSAGE:
        create(message[1]);
        break;
      case SERVER_DELETE_FILE_MESSAGE:
        delete(message[1]);
        break;
      case SERVER_READ_FILE_MESSAGE:
        read(message[1]);
        break;
      case SERVER_APPEND_FILE_MESSAGE:
        append(message[1], String.join(" ", message).substring(message[0].length() + message[1].length() + 2));
        break;
    }
  };

  public ZooKeeper(File ipAddresses, File log) throws IOException {
    //This node accepts all connections from the console. It then handles all commands.
    pool.execute(() -> {
      try (ServerSocket consoleServer = new ServerSocket(HW2Console.PORT)) {
        while (running) {
          AsyncSocketInOutTriple in = new AsyncSocketInOutTriple(consoleServer.accept(), consoleMessageConsumer);
          consoleConnections.add(in);
        }
      } catch (IOException e) {
        handleConsoleOutput(e, true);
      }
    });
    //This node sends all messages to each console, and prints them to standard out.
    pool.execute(() -> {
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
    });
    //Read each file line.
    BufferedReader addressReader = new BufferedReader(new FileReader(ipAddresses));
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
            case "create": {
              handleConsoleOutput("Running create command from log file.");
              create(line.substring(firstWord.length() + 1));
              break;
            }
            case "delete": {
              handleConsoleOutput("Running delete command from log file.");
              delete(line.substring(firstWord.length() + 1));
              break;
            }
            case "append": {
              handleConsoleOutput("Running append command from log file.");
              int secondSpaseIndex = line.indexOf(' ', spaceIndex + 1);
              append(line.substring(firstWord.length() + 1, secondSpaseIndex), line.substring(secondSpaseIndex + 1));
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
    ID = Integer.parseInt(addressReader.readLine());
    handleConsoleOutput("Started node #" + ID);
    String line;
    ArrayList<InetAddress> addressList = new ArrayList<>();
    while ((line = addressReader.readLine()) != null && !(line = line.trim()).equals("")) {
      addressList.add(InetAddress.getByName(line));
    }
    handleConsoleOutput("Successfully read file \"" + ipAddresses.getName() + "\"");
    pool.execute(() -> {
      try {
        try (ServerSocket server = new ServerSocket(INTER_NODE_SETUP_PORT)) {
          while (connections.size() < addressList.size() - 1) {
            try (SocketInOutTriple tmp = new SocketInOutTriple(server.accept())) {
              String nodeName = (String) tmp.in.readObject();
              InetAddress nodeAddress = (InetAddress) tmp.in.readObject();
              connections.put(nodeName, nodeAddress);
              handleConsoleOutput("Node #" + ID + " received the first connection from Node #" + nodeName);
            } catch (ClassNotFoundException e) {
              e.printStackTrace();
            }
          }
        }
      } catch (IOException e) {
        handleConsoleOutput(e, true);
      }
    });
    InetAddress selfAddress = addressList.get(ID);
    for (int i = 0; i < addressList.size(); i++) {
      if (i == ID) continue;
      try (SocketInOutTriple connection = new SocketInOutTriple(new Socket(addressList.get(i), INTER_NODE_SETUP_PORT))) {
        connection.out.writeObject(ID);
        connection.out.writeObject(selfAddress);
        connection.out.flush();
      }
    }
    while (connections.size() < addressList.size() - 1) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {}
    }
  }

  private void handleConsoleOutput(Exception e, boolean shouldDie) {
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

  private void handleConsoleOutput(String message) {
    consoleMessageQueue.add(message);
  }

  private synchronized void append(String tokenName, String line) {
    tokenContents.compute(tokenName, (k, v) -> v + line);
  }

  private boolean twoPhaseCommit(String message[]) {
    List<SocketInOutTriple> sockets = connections.keySet().parallelStream().
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
          s.blockingSendMessage(message);
          return true;
        } catch (IOException e) {
          return false;
        }
      }).collect(Collectors.toList());
    long commitsReceived = sockets.parallelStream().filter(s -> {
      try {
        return Optional.ofNullable(s.blockingRecvMessage()).filter(m -> m[0].equals(VOTE_COMMIT)).isPresent();
      } catch (IOException | ClassNotFoundException e) {
        return false;
      }
    }).count();
    if (commitsReceived != connections.size()) {
      sockets.forEach(s -> {
        try {
          s.blockingSendMessage(ABORT);
        } catch (IOException ignored) {}
      });
      return false;
    } else {
      sockets.forEach(s -> {
        try {
          s.blockingSendMessage(COMMIT);
        } catch (IOException ignored) {}
      });
      return true;
    }
  }

  private synchronized void read(String tokenName) {
    handleConsoleOutput("File \"" + tokenName + "\':\n" + tokenContents.get(tokenName) + "End of File \"" + tokenName + "\"");
  }

  private synchronized void delete(String tokenName) {
    tokenContents.remove(tokenName);
  }

  private synchronized void create(String tokenName) {
    tokenContents.put(tokenName, "");
  }

  /**
   * The ip addresses file should be first, and should contain the ID of this node, and then on each line the IP
   * addresses of each file, their line being their ID+1.
   *
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("usage: ZooKeeper.jar ip_addresses.txt log.txt");
    }
    //Repeatedly create new Zookeepers.
    while (true) {
      //Create new zookeeper, if there is no ip addresses file exit yelling at the user.
      try (ZooKeeper keeper = new ZooKeeper(new File(args[0]), new File(args[0]))) {
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


  @Override
  public void close() {
    running = false;
    consoleConnections.forEach(c -> {if (c.isOpen()) c.close();});
  }
}
