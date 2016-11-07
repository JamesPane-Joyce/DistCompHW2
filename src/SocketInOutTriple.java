import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Useful small class for containing a Socket and the in and out streams that can close them.
 */
public class SocketInOutTriple implements AutoCloseable {
  /**
   * A threadpool for the server.
   */
  protected static final ExecutorService pool = Executors.newCachedThreadPool();
  public final Socket socket;
  public final ObjectInputStream in;
  public final ObjectOutputStream out;
  protected boolean open=true;
  public SocketInOutTriple(Socket socket) throws IOException {
    this.socket=socket;
    this.out=new ObjectOutputStream(socket.getOutputStream());
    this.in= new ObjectInputStream(socket.getInputStream());
  }

  public boolean isOpen(){
    return open;
  }

  @Override
  public void close() {
    open=false;
    try{in.close();}catch(Exception ignored){}
    try{out.close();}catch(Exception ignored){}
    try{socket.close();}catch(Exception ignored){}
  }

  public synchronized boolean blockingSendMessage(String ... exit) throws IOException {
    if(open){
      out.writeObject(exit);
    }
    return open;
  }

  public synchronized @Nullable String[] blockingRecvMessage() throws IOException, ClassNotFoundException {
    if(!open){
      return null;
    }
    return (String[])in.readObject();
  }
}
