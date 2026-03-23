import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class ControllerLogger extends Logger {
    private static ControllerLogger loggerInstance = null;
    private int port;

    public static synchronized void init(LoggingType loggingType, int port) throws IOException {
      if (loggerInstance == null) {
          loggerInstance = new ControllerLogger(loggingType);
          loggerInstance.port = port;
      } else {
          throw new RuntimeException("ControllerLogger already initialised");
      }

    }

    public static ControllerLogger getInstance() {
      if (loggerInstance == null) {
          throw new RuntimeException("ControllerLogger has not been initialised yet");
      } else {
          return loggerInstance;
      }
    }

    protected ControllerLogger(LoggingType loggingType) {
        super(loggingType);
    }

    protected String getLogFileSuffix() {
      return "controller"+port;
   }

    public void addDstore(int port) {
      log("Dstore with port: " + port + " has been added");
   }

    public void removeDstore(int port) {
        log("Dstore with port: " + port + " has been removed");
    }

    public void addClient(int port) {
        log("Client with port: " + port + " has been added");
    }

    public void startStore(String filename,Integer filesize) {
        log("Store operation started for file " + filename + " with size " + filesize);
    }
    public void endStore(String filename, Set<ControllerDstoreConnection> dstoreConnections,
        boolean success) {
        Set<Integer> dstorePorts = new HashSet<>();
        for (ControllerDstoreConnection dstoreConnection : dstoreConnections) {
            dstorePorts.add(dstoreConnection.getPort());
        }
        if (success) {
            log("Store operation successful for file " + filename +
                " at Dstores: " + dstorePorts);
        } else {
            log("Store operation failed for file " + filename + " with success " + success);
        }
    }

    public void startLoad(String filename) {
        log("Load operation started for file " + filename);
    }
    public void endLoad(String filename,Boolean success) {
        if (success) {
            log("Load operation successful for file " + filename);
        } else {
            log("Load operation failed for file " + filename);
        }
    }

    public void startList() {
        log("List operation started");
    }
    public void endList(Set<String> filenames) {
        log("List operation successful with files: " + filenames);
    }

    public void startRemove(String filename) {
        log("Remove operation started for file " + filename);
    }

    public void endRemove(String filename,Boolean success) {
        if (success) {
            log("Remove operation successful for file " + filename);
        } else {
            log("Remove operation failed for file " + filename);
        }
    }

    public void removeClient(int port) {
        log("Client with port: " + port + " has been removed");
    }


}
