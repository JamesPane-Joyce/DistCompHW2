import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.function.Consumer;

/**
 * Useful small class for sending asynchronous messages through a socket.
 * Supply it with a CheckedConsumer and it will consume messages one at a time, can also be supplied with a CheckedBiConsumer
 * which will be fed this class and will have sole read control until it finishes.
 * Tell it to send a message and it will be inserted into a thread safe queue and be sent without another message 
 * interrupting it. You can also insert a message on the listener side that will be consumed.
 *
 * Exits gracefully if it reads null, told to close, or receives an throwable during send or receive which is sent to the
 * throwableConsumer.
 */
@SuppressWarnings("WeakerAccess")
public class ConsumerBasedSocketInOutTriple extends SocketInOutTriple {
  @FunctionalInterface
  public interface CheckedConsumer<A>{
    void accept(A a) throws Throwable;
  }
  @FunctionalInterface
  public interface CheckedBiConsumer<A,B>{
    void accept(A a,B b) throws Throwable;
  }
  /** Thread pool for ConsumerBasedSocketInOutTriples to extract threads from. */
  private static final ExecutorService pool = Executors.newCachedThreadPool();
  /** Thread safe queue into which outgoing message can be chucked. */
  private final TransferQueue<String[]> messageOutQueue = new LinkedTransferQueue<>();
  /** CheckedBiConsumer which will be fed messages that come though one at a time. */
  private final CheckedBiConsumer<ConsumerBasedSocketInOutTriple,String[]> messageConsumer;
  /** Consumer that will be fed any Throwable that pop up during consumption or message sending. */
  private final Consumer<Throwable> throwableConsumer;

  /**
   * Create a new ConsumerBasedSocketInOutTriple around the given socket that will feed messages it reads into the given
   * CheckedBiConsumer.
   * 
   * @param socket The socket this ConsumerBasedSocketInOutTriple is being built around. 
   * @param messageConsumer The CheckedBiConsumer that will be given this and each message that comes through.
   * @param throwableConsumer The Consumer that will be fed any throwables that pop up during consumption.
   * @throws IOException Thrown during the creation of this ConsumerBasedSocketInOutTriple, throwables thrown during
   * consumption or message sending are fed to the consumer.
   */
  public ConsumerBasedSocketInOutTriple(Socket socket,
                                        CheckedBiConsumer<ConsumerBasedSocketInOutTriple,String[]> messageConsumer,
                                        Consumer<Throwable> throwableConsumer) throws IOException {
    super(socket);
    this.messageConsumer = messageConsumer;
    this.throwableConsumer = throwableConsumer;
    pool.execute(() -> {
      try {
        while (open) {
          String[] tmp = messageOutQueue.take();
          if(!open) break;
          blockingSendMessage(tmp);
        }
      } catch (Exception e) {
        if(open) throwableConsumer.accept(e);
      }
      close();
    });
    pool.execute(() -> {
      try {
        String message[];
        while (open){
          message=blockingRecvMessage();
          if(open)processMessage(message);
        }
      } catch (Exception e) {
        if(open) throwableConsumer.accept(e);
      }
      close();
    });
  }

  /**
   * More basic constructor, takes a consumer that only eats messages. Uses a default throwableConsumer that prints a
   * stack trace and System.exits.
   * @param socket The socket this ConsumerBasedSocketInOutTriple is being built around.
   * @param messageConsumer The CheckedConsumer that will be given each message that comes through.
   * @throws IOException Thrown during the creation of this ConsumerBasedSocketInOutTriple, throwables thrown during
   * have their stack trace printed and exit the system..
   */
  public ConsumerBasedSocketInOutTriple(Socket socket, CheckedConsumer<String[]> messageConsumer) throws IOException {
    this(socket,(a,b)->messageConsumer.accept(b),(e)->{e.printStackTrace();System.exit(-1);});
  }

  /**
   * Process message on the receiving side. This blocks further consumers for the duration.
   * @param message The message to be inserted.
   * @return true if the message was non-null and actually processed.
   */
  public boolean processMessage(String... message) {
    if (message == null) return false;
    synchronized (socket) {
      if (open) {
        try {
          messageConsumer.accept(this, message);
        } catch (Throwable t) {
          throwableConsumer.accept(t);
        }
      }
    }
    return open;
  }

  /**
   * Add a message to the queue of messages to send to the other side.
   * @param message The message to send.
   * @return true if this is open at the time of calling and the message was added to the out pile.
   */
  public boolean sendMessage(String... message) {
    if (open) {
      messageOutQueue.add(message);
    }
    return open;
  }

  /**
   * Close this side of the Socket, stop passing through messages, and stop processing message.
   * Ignore all errors at this point.
   */
  public void close() {
    open = false;
    messageOutQueue.clear();
    super.close();
  }
}
