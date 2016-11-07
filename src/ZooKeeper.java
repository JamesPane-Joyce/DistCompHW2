import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.function.Consumer;

/**
 * Created by james on 10/27/16.
 */
public class ZooKeeper implements Consumer<String[]>, AutoCloseable {
  private boolean running=true;
  public static final String SERVER_CREATE_FILE_MESSAGE="CREATE";
  public static final String SERVER_DELETE_FILE_MESSAGE="DELETE";
  public static final String SERVER_READ_FILE_MESSAGE="READ";
  public static final String SERVER_APPEND_FILE_MESSAGE="APPEND";
  public static final String SERVER_END_ZOOKEEPER="EXIT";
  private final Consumer<String[]> serverMessageConsumer =(message)->{
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

  private void append(String tokenName, String line) {}

  private void read(String tokenName) {}

  private void delete(String tokenName) {}

  private void create(String tokenName, String ownerName) {}

  private void setServerConnection(SocketInOutTriple connection){}

  public static void main(String[] args) {
    try (ServerSocket server = new ServerSocket(HW2Console.PORT)) {
      while (true) {
        try(ZooKeeper keeper=new ZooKeeper()){
          try (SocketInOutTriple connection = new SocketInOutTriple(server.accept())) {
            keeper.setServerConnection(connection);
            while(keeper.running){
              Thread.sleep(500);
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          System.err.println("ZOOKEEPER DIED, WE'RE GUCCI.");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("THERE WAS AN ERROR IN SETTING UP THE SERVERSOCKET. SUDOKU IS THE ONLY ANSWER.");
      System.exit(-1);
    }
  }

  @Override
  public void accept(String[] strings) {

  }

  @Override
  public void close(){
    running=false;
  }

  public static HashMap<String,InetAddress> readAddressFile(String fileLocation){
    File ipAddressesFile=new File(fileLocation);
    HashMap<String,InetAddress> nameAddressMap=new HashMap<>();
    try(BufferedReader ipAddressesFileReader=new BufferedReader(new FileReader(ipAddressesFile))){
      String line;
      String[] splitLine;
      while((line=ipAddressesFileReader.readLine())!=null&&!line.trim().equals("")){
        splitLine=line.split(" ");
        nameAddressMap.put(splitLine[0],InetAddress.getByName(splitLine[1]));
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("THERE WAS AN ERROR READING THE FILE CONTAINING THE NAMES OF SERVERS AND THEIR ADDRESSES.");
      System.exit(-1);
    }
    return nameAddressMap;
  }
}
