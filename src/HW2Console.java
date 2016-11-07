import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Small command line script for connecting to Amazon EC2 for the first homework.
 *
 * Requires appropriate credentials in the ~/.aws/credentials file
 *
 * First command line argument should be a file which lists the ip addresses/public domains of the micro instances
 * the second command line argument should be the tree file which gives the links between the instances.
 */
public abstract class HW2Console {
  public static final int PORT=44044;
  public static void main(String[] args){
    if(args.length!=1) {
      System.err.println("usage: HW2Console ip_addresses.txt");
    }
    HashMap<String, InetAddress> serverLocationMap = ZooKeeper.readAddressFile(args[0]);
    ArrayList<NodeConnection> connections=new ArrayList<>();
    try {
      System.out.println("Enter the name of a server to establish a client.\n" +
        "If told to exit or quit this console and all local activity will end.");
      Scanner scanner=new Scanner(System.in);
      String line;
      while(!(line=scanner.nextLine()).equalsIgnoreCase("exit")&&!line.equalsIgnoreCase("quit")){
        if(!serverLocationMap.containsKey(line)){
          System.out.println("There is no server named \""+line+"\"");
        }else {
          try {
            connections.add(new NodeConnection(line,serverLocationMap.get(line)));
          } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("THERE WAS AN ERROR IN ESTABLISHING A NEW CONNECTION TO SERVER \""+line+"\" "+serverLocationMap.get(line));
          }
        }
      }
    }finally {
      connections.forEach(c->{if(c.open())c.close();});
    }
  }
}



