import javax.swing.*;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by james on 10/7/16.
 */
public class NodeConsole implements AutoCloseable {
  /** If this console is currently open. */
  private boolean open = true;
  /** The connection from this console to the ZooKeeperNode. */
  private final ConsumerBasedSocketInOutTriple connection;
  /** The NodeWindow acting as human interface to the remote server. */
  private final NodeWindow window;
  /** The JFrame displaying the window. */
  private final JFrame frame;

  /**
   * When a NodeConsole is created open up a window that will display messages from the Node and open up a socket
   * to that Node. If there is an error close the connection to the Node, close the window,
   * and print the error to system.err.
   *
   * @param name    The name of the Node.
   * @param address The location and socket to connect to the Node.
   */
  public NodeConsole(String name, InetAddress address) throws IOException, ClassNotFoundException {
    this.frame = new JFrame();
    frame.setTitle(name);
    this.window = new NodeWindow(this);
    frame.setContentPane(window.contentPane);
    frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
    frame.setSize(400, 400);
    frame.setVisible(true);
    frame.repaint();
    this.connection = new ConsumerBasedSocketInOutTriple(
      new Socket(address, HW2Console.PORT),
      (a,b)->window.display(b),
      Throwable::printStackTrace);
  }

  public void sendCreateCommand(String filename) throws IOException {
    if (open) {
      connection.blockingSendMessage(ZooKeeperNode.CREATE_FILE_COMMAND, filename);
    }
  }

  public void sendDeleteCommand(String filename) throws IOException {
    if (open) {
      connection.blockingSendMessage(ZooKeeperNode.DELETE_FILE_COMMAND, filename);
    }
  }

  public void sendReadCommand(String filename) throws IOException {
    if (open) {
      connection.blockingSendMessage(ZooKeeperNode.SERVER_READ_FILE_MESSAGE, filename);
    }
  }

  public void sendAppendCommand(String filename, String line) throws IOException {
    if (open) {
      connection.blockingSendMessage(ZooKeeperNode.APPEND_FILE_COMMAND, filename, line);
    }
  }

  public boolean open() {
    return open;
  }

  public void close() {
    if (open) {
      open = false;
      try {
        connection.blockingSendMessage("Exit");
      } catch (Exception ignored) {}
      connection.close();
      frame.dispatchEvent(new WindowEvent(frame, WindowEvent.WINDOW_CLOSING));
    }
  }
}
