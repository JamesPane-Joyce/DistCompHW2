import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.function.Consumer;

/**
 * Useful small class for sending asynchronous messages through a socket.
 */
public class AsyncSocketInOutTriple extends SocketInOutTriple {
  private static final ExecutorService pool = Executors.newCachedThreadPool();
  public final TransferQueue<String[]> messageOutQueue=new LinkedTransferQueue<>();
  private final Consumer<String[]> messageConsumer;
  public AsyncSocketInOutTriple(Socket socket, Consumer<String[]> messageConsumer) throws IOException {
    super(socket);
    this.messageConsumer=messageConsumer;
    pool.execute(()->{
      while (open) {
        try {
          blockingSendMessage(messageOutQueue.take());
        } catch (Exception e) {
          e.printStackTrace();
          close();
        }
      }
    });
    pool.execute(()->{
      try {
        while(insertMessage(blockingRecvMessage()));
      } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
        close();
      }
    });
  }

  public synchronized boolean insertMessage(String ... message){
    if(message==null)return false;
    if(open){
      messageConsumer.accept(message);
    }
    return open;
  }

  public boolean sendMessage(String ... message){
    if(open) {
      messageOutQueue.add(message);
    }
    return open;
  }

  public void close(){
    open=false;
    messageOutQueue.clear();
    super.close();
  }

}
