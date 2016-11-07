import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import static java.lang.System.exit;

/**
 * The server that is run to simulate a Raymond Mutual Exclusion algorithm over a series of tokens.
 */
public class RaymondTokenManager {
  /**
   * A threadpool for the server.
   */
  private static final ExecutorService pool = Executors.newCachedThreadPool();
  /**
   * The port number that Nodes use to connect with each other.
   */
  public static final int INTER_NODE_PORT = HW2Console.PORT + 1;
  /**
   * The name of the node in the tree that this manager represents.
   */
  private final String nodeName;
  /**
   * A map from token names to whether this manager is currently using them. Used to sync the data structures too.
   */
  private final HashMap<String, Boolean> usingResources = new HashMap<>();
  /**
   * A map from token names to the names of neighbor nodes which are closer or are the holders of those tokens. May
   * also contain this manager's name which indicates this manager holds that token.
   */
  private final HashMap<String, String> tokenHolders = new HashMap<>();
  /**
   * A map from token names to whether each token has been requested.
   */
  private final HashMap<String, Boolean> askeds = new HashMap<>();
  /**
   * A map from tokens to the queues of names of neighbor nodes which are requesting the tokens.
   */
  private final HashMap<String, LinkedList<String>> requestQueues = new HashMap<>();
  /**
   * A map from token names to the string that represents their file contents. Should only contain tokens this node
   * currently holds.
   */
  private final Map<String, String> fileContents = Collections.synchronizedMap(new HashMap<>());
  /**
   * A map from the names of neighbor nodes to queues of requests that will be sent, one at a time, to that node.
   */
  private final TreeMap<String, TransferQueue<String[]>> adjacentNodeMessageQueue = new TreeMap<>();
  /**
   * A queue from which messages will be taken and sent one at a time to the console.
   */
  private final TransferQueue<String> consoleMessageQueue = new LinkedTransferQueue<>();
  /**
   * A boolean that can be set to false to tell all threads to stop looping.
   */
  private boolean running = true;

  /**
   * Create a token manager, start up a thread for sending messages to the console and  another thread for accepting new
   * connections from other managers.
   *
   * @param nodeName   The name of this node.
   * @param connection The connection to the console.
   */
  public RaymondTokenManager(String nodeName, SocketInOutTriple connection) {
    this.nodeName = nodeName;
    pool.execute(() -> {
      addToConsoleOutput("Node \"" + nodeName + "\" initialized.");
      try {
      } catch (InterruptedException | IOException e) {
        if (running) {
          e.printStackTrace();
          close();
          exit(-1);
        }
      }
    });
  }

  /**
   * Create a token of the given name, held by the given neighbor or self node.
   *
   * @param tokenName  The name of the token to create.
   * @param holderName The name of the node holding the token.
   */
  public void createToken(String tokenName, String holderName) {
    boolean selfHolding = Objects.equals(nodeName, holderName);
    if (selfHolding) {
      addToConsoleOutput("Node \"" + nodeName + "\" started attempting to create token \"" + tokenName + "\".");
    } else {
      addToConsoleOutput("Node \"" + nodeName + "\" started creating token \"" + tokenName + "\" held by node \"" + holderName + "\".");
    }
    synchronized (usingResources) {
      usingResources.put(tokenName, false);
      tokenHolders.put(tokenName, holderName);
      askeds.put(tokenName, false);
      requestQueues.put(tokenName, new LinkedList<>());
      if (selfHolding) {
        fileContents.put(tokenName, "");
      }
    }
    if (selfHolding) {
      addToConsoleOutput("Node \"" + nodeName + "\" finished creating token \"" + tokenName + "\".");
    } else {
      addToConsoleOutput("Node \"" + nodeName + "\" finished creating token \"" + tokenName + "\" held by node \"" + holderName + "\".");
    }
  }

  /**
   * Delete a token of the given name.
   *
   * @param tokenName The name of the token to be deleted.
   */
  public void deleteToken(String tokenName) {
    addToConsoleOutput("Node \"" + nodeName + "\" started deleting token \"" + tokenName + "\".");
    synchronized (usingResources) {
      usingResources.remove(tokenName);
      tokenHolders.remove(tokenName);
      askeds.remove(tokenName);
      requestQueues.remove(tokenName);
      fileContents.remove(tokenName);
    }
    addToConsoleOutput("Node \"" + nodeName + "\" finished deleting token \"" + tokenName + "\".");
  }

  /**
   * Assign a token to the next requester if this node possesses that token and is not using it.
   *
   * @param tokenName The name of the token to try passing.
   */
  public void assignToken(String tokenName) {
    addToConsoleOutput("Node \"" + nodeName + "\" started assigning token \"" + tokenName + "\".");
    synchronized (usingResources) {
      LinkedList<String> requestQueue = requestQueues.get(tokenName);
      //addToConsoleOutput("*"+Objects.equals(tokenHolders.get(tokenName), nodeName)+"*"+!usingResources.get(tokenName)+"*"+!requestQueue.isEmpty()+"*");
      if (Objects.equals(tokenHolders.get(tokenName), nodeName)
        && !usingResources.get(tokenName)
        && !requestQueue.isEmpty()) {
        String nextTokenHolder = requestQueue.removeFirst();
        tokenHolders.put(tokenName, nextTokenHolder);
        askeds.put(tokenName, false);
        if (Objects.equals(nextTokenHolder, nodeName)) {
          usingResources.put(tokenName, true);
          addToConsoleOutput("Node \"" + nodeName + "\" assigned token \"" + tokenName + "\" to itself.");
        } else {
          //addToConsoleOutput("****"+tokenName+"***"+fileContents.get(tokenName)+"****");
          adjacentNodeMessageQueue.get(nextTokenHolder).add(new String[]{"token", tokenName, fileContents.remove(tokenName)});
          addToConsoleOutput("Node \"" + nodeName + "\" assigned token \"" + tokenName + "\" to \"" + nextTokenHolder + "\".");
        }
      }
    }
    addToConsoleOutput("Node \"" + nodeName + "\" finished assigning token \"" + tokenName + "\".");
  }

  /**
   * Send a request for the given token to the appropriate holder.
   *
   * @param tokenName The name of the token to be requested.
   */
  public void sendRequest(String tokenName) {
    addToConsoleOutput("Node \"" + nodeName + "\" started sending a request for token \"" + tokenName + "\".");
    synchronized (usingResources) {
      String tokenHolder = tokenHolders.get(tokenName);
      if (!Objects.equals(tokenHolder, nodeName)
        && !requestQueues.get(tokenName).isEmpty()
        && !askeds.get(tokenName)) {
        adjacentNodeMessageQueue.get(tokenHolder).add(new String[]{"request", tokenName});
        askeds.put(tokenName, true);
        addToConsoleOutput("Node \"" + nodeName + "\" finished sending a request for token \"" + tokenName + "\".");
      } else {
        addToConsoleOutput("Node \"" + nodeName + "\" did not send a request for token \"" + tokenName + "\".");
      }
    }
  }

  /**
   * Queue a request for the given token, then send along a request when the time comes.
   *
   * @param tokenName The name of the token to request.
   */
  public void requestResource(String tokenName) {
    addToConsoleOutput("Node \"" + nodeName + "\" started queueing a request for token \"" + tokenName + "\".");
    synchronized (this) {
      synchronized (usingResources) {
        requestQueues.get(tokenName).addLast(nodeName);
        assignToken(tokenName);
        sendRequest(tokenName);
      }
    }
    addToConsoleOutput("Node \"" + nodeName + "\" finished queueing a request for token \"" + tokenName + "\".");
  }

  /**
   * Release the given token to the next requester.
   *
   * @param tokenName The token to release.
   */
  public void releaseResource(String tokenName) {
    addToConsoleOutput("Node \"" + nodeName + "\" started releasing token \"" + tokenName + "\".");
    synchronized (this) {
      synchronized (usingResources) {
        usingResources.put(tokenName, false);
        assignToken(tokenName);
        sendRequest(tokenName);
      }
    }
    addToConsoleOutput("Node \"" + nodeName + "\" finished releasing token \"" + tokenName + "\".");
  }

  /**
   * Process a request from another node for a token.
   *
   * @param requester The name of the node that made the request.
   * @param tokenName The name of the token that has been requested.
   */
  public void receiveRequest(String requester, String tokenName) {
    addToConsoleOutput("Node \"" + nodeName + "\" started processing a request from node \"" + requester + "\" for token \"" + tokenName + "\".");
    synchronized (this) {
      synchronized (usingResources) {
        requestQueues.get(tokenName).add(requester);
        assignToken(tokenName);
        sendRequest(tokenName);
      }
    }
    addToConsoleOutput("Node \"" + nodeName + "\" finished processing a request from node \"" + requester + "\" for token \"" + tokenName + "\".");
  }

  /**
   * Receive the given token with the given contents from a neighbor node.
   *
   * @param tokenName The name of the token.
   * @param contents  The contents of the token.
   */
  public void receiveToken(String tokenName, String contents) {
    addToConsoleOutput("Node \"" + nodeName + "\" started processing \"" + tokenName + "\" which it just received.");
    synchronized (this) {
      synchronized (usingResources) {
        tokenHolders.put(tokenName, nodeName);
        fileContents.put(tokenName, contents);
        assignToken(tokenName);
        sendRequest(tokenName);
      }
    }
    addToConsoleOutput("Node \"" + nodeName + "\" finished processing \"" + tokenName + "\" which it just received.");
  }

  /**
   * Read the contents of the file protected by the given token name. This blocks until this node holds the token.
   *
   * @param tokenName The token to read.
   * @return The contents of the file.
   */
  public String readFile(String tokenName) {
    addToConsoleOutput("Node \"" + nodeName + "\" started processing a read request for \"" + tokenName + "\".");
    requestResource(tokenName);
    while (!Objects.equals(nodeName, tokenHolders.get(tokenName))) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {}
    }
    String fileString;
    synchronized (usingResources) {
      fileString = fileContents.get(tokenName);
    }
    releaseResource(tokenName);
    addToConsoleOutput("Node \"" + nodeName + "\" finished processing a read request for \"" + tokenName + "\".");
    return fileString;
  }

  /**
   * Append a line of text to the given file. The newline character is added within this method. This blocks until this
   * node holds the token.
   *
   * @param tokenName The name of the token to append.
   * @param input     The line of text to append ot the file.
   */
  public void requestAppend(String tokenName, String input) {
    addToConsoleOutput("Node \"" + nodeName + "\" started appending line \"" + input + "\" to  \"" + tokenName + "\".");
    requestResource(tokenName);
    while (!Objects.equals(nodeName, tokenHolders.get(tokenName))) {
      try {Thread.sleep(100);} catch (InterruptedException ignore) {}
    }
    synchronized (usingResources) {
      fileContents.compute(tokenName,(k,v)->v==null||v.equals("")?input:v+"\n"+input);
    }
    releaseResource(tokenName);
    addToConsoleOutput("Node \"" + nodeName + "\" finished appending line \"" + input + "\" to  \"" + tokenName + "\".");
  }

  /**
   * Establish a connection to a named foreign node at the given remote address. Threads on this node and the other are
   * created to handle messages.
   *
   * @param otherNodeName    The name given to the other node.
   * @param otherNodeAddress The address of the other node.
   * @throws IOException Thrown if there is some issue in establishing the connection.
   */
  public void establishConnection(String otherNodeName, InetAddress otherNodeAddress) throws IOException {
    addToConsoleOutput("Node \"" + nodeName + "\" established a connection to \"" + otherNodeName + "\" at location " + otherNodeAddress + ".");
    SocketInOutTriple foreignNodeConnection =
      new SocketInOutTriple(new Socket(otherNodeAddress, INTER_NODE_PORT));
    foreignNodeConnection.out.writeObject(nodeName);
    foreignNodeConnection.out.flush();
    startSlaveThreads(otherNodeName, foreignNodeConnection);
  }

  /**
   * Start up all the poor little threads that send and receive requests from the given node over the given connection.
   *
   * @param otherNodeName The name of the other node.
   * @param connection    The connection that communicates between the two nodes.
   */
  private void startSlaveThreads(String otherNodeName, SocketInOutTriple connection) {
    LinkedTransferQueue<String[]> queue = new LinkedTransferQueue<>();
    adjacentNodeMessageQueue.put(otherNodeName, queue);
    pool.execute(() -> {
      while (running) {
        try {
          String[] nextCommand = queue.take();
          for (String word : nextCommand) {
            connection.out.writeObject(word);
          }
          connection.out.flush();
        } catch (InterruptedException | IOException e) {
          e.printStackTrace();
          close();
          exit(-1);
        }
      }
    });
    pool.execute(() -> {
      try {
        while (running) {
          String command = (String) connection.in.readObject();
          String tokenName = (String) connection.in.readObject();
          switch (command) {
            case "created": {
                boolean exists;
                synchronized (usingResources) {
                  exists = usingResources.containsKey(tokenName);
                }
                if (!exists) {
                  createToken(tokenName, otherNodeName);
                  adjacentNodeMessageQueue.forEach((k, v) -> {
                    if (k.equals(otherNodeName)) return;
                    v.add(new String[]{"created", tokenName});
                  });
                }
              }
              break;
            case "deleted": {
                boolean exists;
                synchronized (usingResources) {
                  exists = usingResources.containsKey(tokenName);
                }
                if (exists) {
                  deleteToken(tokenName);
                  adjacentNodeMessageQueue.forEach((k, v) -> {
                    if (k.equals(otherNodeName)) return;
                    v.add(new String[]{"deleted", tokenName});
                  });
                }
              }
              break;
            case "request":
              receiveRequest(otherNodeName, tokenName);
              break;
            case "token":
              String contents = (String) connection.in.readObject();
              receiveToken(tokenName, contents);
              break;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        close();
        exit(-1);
      }
    });
  }

  public void close() {
    running = false;
  }
  public void addToConsoleOutput(String message){
    consoleMessageQueue.add(message);
    System.out.println(message);
    System.out.flush();
  }

  public static void main(String[] args) {
    while (true) {
      try (ServerSocket server = new ServerSocket(HW2Console.PORT)) {
        try (SocketInOutTriple connection = new SocketInOutTriple(server.accept())) {
          String name = (String) connection.in.readObject();
          RaymondTokenManager manager = new RaymondTokenManager(name, connection);
          String command;
          while ((command = (String) connection.in.readObject()) != null && !command.equals("Exit")) {
            switch (command) {
              case "create": {
                  String tokenName = (String) connection.in.readObject();
                  boolean exists;
                  synchronized (manager.usingResources) {
                    exists=manager.usingResources.containsKey(tokenName);
                  }
                  if (!exists) {
                    manager.createToken(tokenName, manager.nodeName);
                    manager.adjacentNodeMessageQueue.forEach((k, v) -> v.add(new String[]{"created", tokenName}));
                  } else {
                    manager.addToConsoleOutput("Node \"" + manager.nodeName + "\" attempted to create token \"" + tokenName + "\" which already exists.");
                  }
                }
                break;
              case "delete": {
                  String tokenName = (String) connection.in.readObject();
                  boolean exists;
                  synchronized (manager.usingResources) {
                    exists=manager.usingResources.containsKey(tokenName);
                  }
                  if (exists) {
                    manager.deleteToken(tokenName);
                    manager.adjacentNodeMessageQueue.forEach((k, v) -> v.add(new String[]{"deleted", tokenName}));
                  } else {
                    manager.addToConsoleOutput("Node \"" + manager.nodeName + "\" attempted to delete token \"" + tokenName + "\" which does not exist.");
                  }
                }
                break;
              case "read": {
                  String tokenName = (String) connection.in.readObject();
                  boolean exists;
                  synchronized (manager.usingResources) {
                    exists=manager.usingResources.containsKey(tokenName);
                  }
                  if (exists) {
                    manager.addToConsoleOutput("Read from file \"" + tokenName + "\":\n" + manager.readFile(tokenName) + "\nEnd of file: " + tokenName);
                  } else {
                    manager.addToConsoleOutput("Node \"" + manager.nodeName + "\" attempted to read token \"" + tokenName + "\" which does not exist.");
                  }
                }
                break;
              case "append": {
                  String tokenName = (String) connection.in.readObject();
                  boolean exists;
                  synchronized (manager.usingResources) {
                    exists=manager.usingResources.containsKey(tokenName);
                  }
                  if (exists) {
                    String toAppend = (String) connection.in.readObject();
                    manager.requestAppend(tokenName, toAppend);
                  } else {
                    manager.addToConsoleOutput("Node \"" + manager.nodeName + "\" attempted to append to token \"" + tokenName + "\" which does not exist.");
                  }
                }
                break;
              case "Establish Connection": {
                  String otherNodeName = (String) connection.in.readObject();
                  InetSocketAddress otherNodeAddress = (InetSocketAddress) connection.in.readObject();
                  manager.establishConnection(otherNodeName, otherNodeAddress.getAddress());
                }
                break;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }
  }
}