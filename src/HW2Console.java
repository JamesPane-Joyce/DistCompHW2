import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Small command line script for connecting to Amazon EC2 for the second homework.
 * <p>
 * Requires appropriate credentials in the ~/.aws/credentials file
 * <p>
 * First command line argument should be a file which lists the ip addresses/public domains of the micro instances
 * the second command line argument should be the tree file which gives the links between the instances.
 */
public abstract class HW2Console {
  public static final int PORT = 44044;

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("usage: HW2Console ip_addresses.txt");
    }
    HashMap<String, InetAddress> serverLocationMap = readAddressFile(args[0]);
    ArrayList<NodeConsole> connections = new ArrayList<>();
    try {
      System.out.println("Enter the name of a server to establish a client.\n" +
        "If told to exit or quit this console and all local activity will end.");
      Scanner scanner = new Scanner(System.in);
      String line;
      while (!(line = scanner.nextLine().trim()).equalsIgnoreCase("exit") && !line.equalsIgnoreCase("quit")) {
        if (!serverLocationMap.containsKey(line)) {
          System.out.println("There is no server named \"" + line + "\"");
        } else {
          try {
            connections.add(new NodeConsole(line, serverLocationMap.get(line)));
          } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("THERE WAS AN ERROR IN ESTABLISHING A NEW CONNECTION TO SERVER \"" + line + "\" " + serverLocationMap.get(line));
          }
        }
      }
    } finally {
      connections.forEach(c -> {if (c.open()) c.close();});
    }
  }

  /**
   * Read the file at the given location, each line of which is the name of a node followed by a space, and then the IP
   * address of that node.
   * @param addressLocation The String of the path to the ipAddressesFile.
   * @return A HashMap from the names of nodes to their InetAddresses.
   */
  public static HashMap<String, InetAddress> readAddressFile(String addressLocation) {
    File ipAddressesFile = new File(addressLocation);
    HashMap<String, InetAddress> nameAddressMap = new HashMap<>();
    try (BufferedReader ipAddressesFileReader = new BufferedReader(new FileReader(ipAddressesFile))) {
      String line;
      String[] splitLine;
      while ((line = ipAddressesFileReader.readLine()) != null && !line.trim().equals("")) {
        splitLine = line.split(" ");
        nameAddressMap.put(splitLine[0], InetAddress.getByName(splitLine[1]));
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("THERE WAS AN ERROR READING THE FILE CONTAINING THE NAMES OF SERVERS AND THEIR ADDRESSES.");
      System.exit(-1);
    }
    return nameAddressMap;
  }
}



