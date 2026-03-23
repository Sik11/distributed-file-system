import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ControllerDstoreConnection extends Connection{
  private Controller controller;

  String initial;

  private final Set<String> files;

  private Integer dstore;


  public ControllerDstoreConnection(Controller controller, Socket input, Integer port,
      String initial) throws Exception {
    super(input,input);
    this.dstore = port;
    this.controller = controller;
    this.initial = initial;
    this.files = ConcurrentHashMap.newKeySet();
  }

  @Override
  protected void handleMessage(String message) {
    String[] parts = message.split(" ");
    String command = parts[0];
    switch (command) {
        case Protocol.STORE_ACK_TOKEN -> {
          if (parts.length == 2) {
            this.controller.getIndex().addStoreAck(parts[1]);
          } else {
            System.err.println("Invalid STORE_ACK command");
          }
        }
        case Protocol.REMOVE_ACK_TOKEN ->  {
          if (parts.length == 2) {
            this.controller.getIndex().addRemoveAck(parts[1]);
          } else {
            System.err.println("Invalid REMOVE_ACK command");
          }
        }
        case Protocol.LIST_TOKEN -> {
          if (parts.length == 2 || parts.length == 1) {
            if (parts.length == 2) {
              List<String> files = new ArrayList<>(Arrays.asList(parts[1].split(",")));
              this.controller.addListAck(getPort(), files);
            } else {
              this.controller.addListAck(getPort(), new ArrayList<>());
            }
          } else {
            System.err.println("Invalid LIST command");
          }
        }
      }
    }


  @Override
  public String read() {
    String message = super.read();
    if (message != null)
      ControllerLogger.getInstance().messageReceived(this.input, message);
    return message;
  }

  public void send(String message) {
    try {
      PrintWriter writer = new PrintWriter(getOutputSocket().getOutputStream(), true);
      writer.println(message);
    } catch (IOException e) {
      System.err.println("Can't send message to client: " + e.getMessage());
    }
    ControllerLogger.getInstance().messageSent(this.output, message);
  }

  public Integer getPort() {
    return this.dstore;
  }

  public void store(String filename) {
    this.files.add(filename);
  }

  public void remove(String filename) {
    this.files.remove(filename);
  }

  public Integer getNumberOfFiles() {
    return this.files.size();
  }



  @Override
  protected void onDisconnect() {
    this.controller.removeDstoreConnection(this);
  }

  public void run() {
    if (this.initial != null) {
      handleMessage(this.initial);
    }

    super.run();
  }


}
