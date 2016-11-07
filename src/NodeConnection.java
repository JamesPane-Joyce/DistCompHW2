import javax.swing.*;
import java.awt.event.WindowEvent;
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by james on 10/7/16.
 */
public class NodeConnection implements AutoCloseable{
  private final String name;
  private boolean open =true;
  private final SocketInOutTriple connection;
  private final InetAddress address;
  private final NodeWindow window;
  private final JFrame frame;

  /**
   * When a NodeConnection is created open up a window that will display messages from the Node and open up a socket
   * to that Node. If there is an error close the connection to the Node, close the window,
   * and print the error to system.err.
   * @param name The name of the Node.
   * @param address The location and socket to connect to the Node.
   */
  public NodeConnection(String name, InetAddress address) throws IOException, ClassNotFoundException {
    this.name=name;
    this.address=address;
    this.frame=new JFrame();
    frame.setTitle(name);
    this.window=new NodeWindow(this);
    frame.setContentPane(window.contentPane);
    frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
    frame.setSize(400,400);
    frame.setVisible(true);
    frame.repaint();
    this.connection= new SocketInOutTriple(new Socket(address,HW2Console.PORT),window::display);
  }

  public void sendCreateCommand(String filename) {
    if(open) {
      connection.sendMessage("create",filename);
    }
  }

  public void sendDeleteCommand(String filename) {
    if(open) {
      connection.sendMessage("delete",filename);
    }
  }

  public void sendReadCommand(String filename) {
    if(open) {
      connection.sendMessage("read",filename);
    }
  }

  public void sendAppendCommand(String filename, String line) {
    if(open) {
      connection.sendMessage("append",filename,line);
    }
  }

  public boolean open() {
    return open;
  }

  public void close() throws IOException {
    if (open) {
      open = false;
      connection.blockingSendMessage("Exit");
      connection.close();
      frame.dispatchEvent(new WindowEvent(frame, WindowEvent.WINDOW_CLOSING));
    }
  }
}
