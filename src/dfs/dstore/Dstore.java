package dfs.dstore;

import dfs.logging.DstoreLogger;
import dfs.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Dstore extends ServerSocket{

  private Integer cport;

  private Integer port;

  private Integer timeout;

  private String filefolder;

  private final Set<String> fileSet;

  private Socket dsocket;

  private DstoreClientConnection controllerConnection;

  public Dstore(Integer port, Integer cport, Integer timeout, String filefolder)
      throws IOException {
    super(port);
    this.cport = cport;
    this.timeout = timeout;
    this.filefolder = filefolder;
    this.port = port;
    dsocket = new Socket(InetAddress.getLoopbackAddress(), cport);
    this.fileSet = ConcurrentHashMap.newKeySet();

    initialiseFolder();
    DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL,this.port);
    new Thread(() -> {
      try {
        Socket controller = accept();
        DstoreLogger.getInstance().connectionAccepted(controller);
        controllerConnection = new DstoreClientConnection(Dstore.this, dsocket, dsocket);
        new Thread(controllerConnection).start();
        listen();
      } catch (Exception e) {
        DstoreLogger.getInstance().error("Error connecting to controller");
      }
    }).start();
    PrintWriter out = new PrintWriter(dsocket.getOutputStream(), true);
    out.println("JOIN " + port);
    DstoreLogger.getInstance().messageSent(dsocket, "JOIN " + port);
  }

  public void listen() {
    while (true) {
      try {
        onAccept(accept());
      } catch (Exception e) {
        DstoreLogger.getInstance().error("Cannot accept connection");
      }
    }
  }

  public void onAccept(Socket socket) {
    DstoreLogger.getInstance().connectionAccepted(socket);
    try {
      new Thread(new DstoreClientConnection(this, socket,socket)).start();
    } catch (Exception e) {
      DstoreLogger.getInstance().error(e.getMessage());
    }
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Error: invalid number of arguments");
      return;
    }

    Integer port;
    Integer controllerPort;
    Integer timeout;
    try {
      port = Integer.parseInt(args[0]);
      controllerPort = Integer.parseInt(args[1]);
      timeout = Integer.parseInt(args[2]);
    } catch (NumberFormatException e) {
      System.err.println("Unable to parse values");
      return;
    }

    try {
      Dstore dstore = new Dstore(port, controllerPort, timeout, args[3]);
    } catch (IOException e) {
      System.err.println("Unable to start Dstore");
    }


  }

  private void initialiseFolder() {
    File folder = new File(this.filefolder);
    if (!folder.isDirectory()) {
      folder.mkdirs();
    } else {
      for (File f : folder.listFiles())
        f.delete();
    }
  }

  public void uploadFile(String filename) {
    this.fileSet.add(filename);
  }

  public void removeFile(String filename) {
    this.fileSet.remove(filename);
  }

public Set<String> getFileSet() {
    return this.fileSet;
  }

  public Integer getPort() {
    return this.port;
  }

  public DstoreClientConnection getControllerConnection() {
    return this.controllerConnection;
  }

  public String getFileFolder() {
    return this.filefolder;
  }

  public Socket getSocket() {
    return this.dsocket;
  }


}
