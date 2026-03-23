import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class DstoreClientConnection extends Connection{
  private Dstore dstore;

  public DstoreClientConnection(Dstore dstore, Socket input, Socket output) throws Exception {
    super(input, output);
    this.dstore = dstore;
  }

  @Override
  protected void handleMessage(String message) {
    String[] parts = message.split(" ");
    String command = parts[0];

    switch (command) {
      case Protocol.STORE_TOKEN -> {
        if (parts.length == 3) {
          try {
            Integer fileSize = Integer.parseInt(parts[2]);
            File outputFile = new File(dstore.getFileFolder() + "/" + parts[1]);
            send(Protocol.ACK_TOKEN);
            byte[] buf = input.getInputStream().readNBytes(fileSize);
            FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
            fileOutputStream.write(buf);
            fileOutputStream.close();

            dstore.uploadFile(parts[1]);
            dstore.getControllerConnection().send(Protocol.STORE_ACK_TOKEN + " " + parts[1]);
          } catch (NumberFormatException e) {
            System.err.println(Protocol.STORE_TOKEN + " file_size not number");
            throw e;
          } catch (Exception e) {
            System.err.println("Can't read file contents" + e.getMessage());
          }
        }
      }
      case Protocol.LOAD_DATA_TOKEN -> {
        if (parts.length == 2) {
          try {
            File file = new File(dstore.getFileFolder() + "/" + parts[1]);
            if (file.exists()) {
              byte[] buf = new byte[(int) file.length()];
              FileInputStream fileInputStream = new FileInputStream(file);
              fileInputStream.read(buf);
              fileInputStream.close();
              output.getOutputStream().write(buf);
            } else {
              send("ERR_FILE_NOT_FOUND");
            }
          } catch (Exception e) {
            System.err.println("Can't read file contents" + e.getMessage());
          }
        } else {
          System.err.println("Invalid LOAD_DATA_TOKEN message");
        }
      }
      case Protocol.REMOVE_TOKEN -> {
        if (parts.length == 2) {
          try {
            File file = new File(dstore.getFileFolder() + "/" + parts[1]);
            if (file.exists()) {
              file.delete();
              dstore.removeFile(parts[1]);
              dstore.getControllerConnection().send(Protocol.REMOVE_ACK_TOKEN + " " + parts[1]);
            } else {
              dstore.getControllerConnection().send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + parts[1]);
            }
          } catch (Exception e) {
            System.err.println("Can't read file contents" + e.getMessage());
          }
        } else {
          System.err.println("Invalid LOAD_DATA_TOKEN message");
        }
      }
      case Protocol.LIST_TOKEN -> {
        if (parts.length == 1) {
          try {
            File folder = new File(dstore.getFileFolder());
            File[] listOfFiles = folder.listFiles();
            String files = "";
            for (File file : listOfFiles) {
              if (file.isFile()) {
                files += file.getName() + ",";
              }
            }
            dstore.getControllerConnection().send(Protocol.LIST_TOKEN + " " + files);
          } catch (Exception e) {
            System.err.println("Can't read file contents" + e.getMessage());
          }
        } else {
          System.err.println("Invalid LIST_TOKEN message");
        }
      }
    }
  }

  @Override
  public String read() {
    String message = super.read();
    if (message != null)
      DstoreLogger.getInstance().messageReceived(this.input, message);
    return message;
  }

  public void send(String message) {
    super.send(message);
    DstoreLogger.getInstance().messageSent(this.output, message);
  }



  @Override
  protected void onDisconnect() {

  }
}
