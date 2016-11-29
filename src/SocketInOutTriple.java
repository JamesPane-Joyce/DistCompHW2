import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Useful small class for containing a Socket and the in and out streams that can close them.
 */
@SuppressWarnings("WeakerAccess")
public class SocketInOutTriple implements AutoCloseable {
  /** The socket encapsulated by this triple. */
  public final Socket socket;
  /** The input stream that can be manually used to read stuff. */
  public final ObjectInputStream in;
  /** The output stream that cna be manually used to write stuff. */
  public final ObjectOutputStream out;
  /** Whether this socket is currently open. */
  protected boolean open = true;

  public SocketInOutTriple(Socket socket) throws IOException {
    this.socket = socket;
    this.out = new ObjectOutputStream(socket.getOutputStream());
    this.in = new ObjectInputStream(socket.getInputStream());
  }

  public boolean isOpen() {
    return open;
  }

  /**
   * Close quietly, ignoring any errors that come up because this is closed and we don't really care.
   */
  @Override
  public void close() {
    open = false;
    try {in.close();} catch (Exception ignored) {}
    try {out.close();} catch (Exception ignored) {}
    try {socket.close();} catch (Exception ignored) {}
  }

  /**
   * Send a message, prevent any other message or objects from being sent for the duration.
   * @param message The message to send.
   * @return true if this socket was open and sent the message.
   * @throws IOException Any errors that occurred during sending.
   */
  public synchronized boolean blockingSendMessage(String... message) throws IOException {
    if (open) {
      blockingSendObject(message);
    }
    return open;
  }

  /**
   * Send an object, prevent any other objects or messages from being sent for the duration.
   * @param o The object to send.
   * @return true if this socket was open and sent the object.
   * @throws IOException Any errors that occurred during sending.
   */
  public synchronized boolean blockingSendObject(Object o) throws IOException {
    if(open){
      out.writeObject(o);
      out.flush();
    }
    return open;
  }

  /**
   * Receive a message.
   * @return The message received, null if this was closed during the receive.
   * @throws IOException Any errors that occurred during receiving.
   */
  public synchronized String[] blockingRecvMessage() throws IOException, ClassNotFoundException {
    if (!open) {
      return null;
    }
    return (String[]) in.readObject();
  }
}
