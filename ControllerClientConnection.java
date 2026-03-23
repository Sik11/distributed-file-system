import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ControllerClientConnection extends Connection{
  private Controller controller;

  String initial;

  private Map<String,Integer> fileLoadCount;


  public ControllerClientConnection(Controller controller, Socket socket,
      String initial) throws Exception {
    super(socket,socket);
    this.controller = controller;
    this.initial = initial;
    this.fileLoadCount = new ConcurrentHashMap<>();
  }

  @Override
  protected void handleMessage(String message) {
    String[] parts = message.split(" ");
    String command = parts[0];
    switch (command) {
      case Protocol.LIST_TOKEN -> {
        if (parts.length == 1) {
          controller.list(this);
        } else {
          System.err.println("Invalid LIST command");
        }
      }
      case Protocol.STORE_TOKEN -> {
          if (parts.length == 3) {
            try {
              controller.store(this, parts[1], Integer.parseInt(parts[2]));
            } catch (NumberFormatException e) {
              System.err.println("File size is not a number");
            }
          } else {
            System.err.println("Invalid STORE command");
          }
      }
      case Protocol.LOAD_TOKEN -> {
        if (parts.length == 2) {
          fileLoadCount.put(parts[1], 0);
          controller.load(this, parts[1], 0);
        } else {
          System.err.println("Invalid LOAD command");
        }
      }
      case Protocol.RELOAD_TOKEN -> {
        if (parts.length == 2) {
          if (fileLoadCount.containsKey(parts[1])) {
            fileLoadCount.put(parts[1], fileLoadCount.get(parts[1]) + 1);
            controller.load(this, parts[1], fileLoadCount.get(parts[1]));
          } else {
            System.err.println("File has never been loaded before");
          }
        } else {
          System.err.println("Invalid RELOAD command");
        }
      }
      case Protocol.REMOVE_TOKEN -> {
        if (parts.length == 2) {
          controller.remove(this, parts[1]);
        } else {
          System.err.println("Invalid REMOVE command");
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
    super.send(message);
    ControllerLogger.getInstance().messageSent(this.output, message);
  }



  @Override
  protected void onDisconnect() {
    this.controller.removeClientConnection(this);
  }

  public int getPort() {
    return this.getOutputSocket().getPort();
  }



  public void run() {
    if (this.initial != null)
      handleMessage(this.initial);
    super.run();
  }


}

