import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;

/**
 * Created by james on 10/27/16.
 */
public class ZooKeeper implements AutoCloseable {
  private boolean running=true;
  private static final ExecutorService pool = Executors.newCachedThreadPool();
  public static final int INTER_NODE_PORT = HW2Console.PORT + 255;
  public static final String SERVER_CREATE_FILE_MESSAGE="CREATE";
  public static final String SERVER_DELETE_FILE_MESSAGE="DELETE";
  public static final String SERVER_READ_FILE_MESSAGE="READ";
  public static final String SERVER_APPEND_FILE_MESSAGE="APPEND";
  public static final String SERVER_END_ZOOKEEPER="EXIT";
  public final List<AsyncSocketInOutTriple> consoleConnections = Collections.synchronizedList(new ArrayList<>());
  private final int ID;
  private final LinkedTransferQueue<String> consoleMessageQueue = new LinkedTransferQueue<>();
  private final HashMap<String, SocketInOutTriple> connections = new HashMap<>();
  private final Consumer<String[]> consoleMessageConsumer = (message) -> {
    if(message==null||message.length==0) return;
    if(message.length==1&&(message[0].equals(SERVER_END_ZOOKEEPER))) {
      try { close(); } catch (Exception ignored) {}
    }
    switch (message[0]) {
      case SERVER_CREATE_FILE_MESSAGE:
        create(message[1],message[2]);
        break;
      case SERVER_DELETE_FILE_MESSAGE:
        delete(message[1]);
        break;
      case SERVER_READ_FILE_MESSAGE:
        read(message[1]);
        break;
      case SERVER_APPEND_FILE_MESSAGE:
        append(message[1],message[2]);
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
    Runnable consoleMessageSender = () -> {
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
        } catch (InterruptedException ignored) {
          continue;
        }
      }
    };
    pool.execute(consoleMessageSender);
    //Read each file line.
    BufferedReader addressReader = new BufferedReader(new FileReader(ipAddresses));
    //TODO startup read log code
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
        try (ServerSocket server = new ServerSocket(INTER_NODE_PORT)) {
          while (connections.size() < addressList.size() - 1) {
            SocketInOutTriple tmp = new SocketInOutTriple(server.accept());
            try {
              String[] message = tmp.blockingRecvMessage();
              if (message == null) {
                throw new Exception("The first connection established by a node died before it sent a name.");
              } else {
                connections.put(message[0], tmp);
                handleConsoleOutput("Node #" + ID + " received the first connection from Node #" + message[0]);
              }
            } catch (Exception e) {
              handleConsoleOutput(e, true);
            }
          }
          while (running) {
            SocketInOutTriple tmp = new SocketInOutTriple(server.accept());
            try {
              String[] message = tmp.blockingRecvMessage();
              if (message == null) {
                continue;
              } else {
                connections.put(message[0], tmp);
                handleConsoleOutput("Node #" + ID + " received a new connection from Node #" + message[0]);
              }
            } catch (Exception e) {
              handleConsoleOutput(e, true);
            }
          }
        }
      } catch (IOException e) {
        handleConsoleOutput(e, true);
      }
    });
    for (int i = 0; i < addressList.size(); i++) {
      if (i == ID) continue;
      SocketInOutTriple connection = new SocketInOutTriple(new Socket(addressList.get(i), INTER_NODE_PORT));
      connection.blockingSendMessage(ID + "");
      connections.put(ID + "", connection);
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

  private synchronized void append(String tokenName, String line) {}

  private synchronized void read(String tokenName) {}

  private synchronized void delete(String tokenName) {}

  private synchronized void create(String tokenName, String ownerName) {}

  private void setServerConnection(SocketInOutTriple connection){}

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
  public void close(){
    running=false;
  }
}
