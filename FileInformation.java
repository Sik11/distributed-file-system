import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class FileInformation {

  private Set<Socket> dstores = ConcurrentHashMap.newKeySet(); // List of
  // DStores
  // where the
  // file is being stored
  private Integer fileSize; // File size
  private String state;

  private Socket owner;

  private Map<String, FileInformation> index = new ConcurrentHashMap<>();
// State of file storage (e.g., "store in progress", "store complete")

  // Constructor
  public FileInformation(Socket owner, Set<Socket> dstores, Integer fileSize, String state) {
    this.owner = owner;
    this.dstores = dstores;
    this.fileSize = fileSize;
    this.state = state;
  }

  public Integer getFileSize() {
    return fileSize;
  }

  public Set<Socket> getDstores() {
    return dstores;
  }

  public String getState() {
    return state;
  }

  public Socket getOwner() {
    return owner;
  }

  public void setOwner(Socket owner) {
    this.owner = owner;
  }

  public void setFileSize(Integer fileSize) {
    this.fileSize = fileSize;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String toString() {
    String ownerstr = "Owner: " + owner;
    String filesize = "File size: " + fileSize + " bytes";
    String dstorestr = "DStores: " + dstores;
    String statestr = "State: " + state;
    return "[ " + ownerstr + ", " + filesize + ", " + dstorestr + ", " + statestr + "]";
  }

  public void addDStore(Socket dstore) {
    this.dstores.add(dstore);
  }
  public void removeDStore(Socket dstore) {
    this.dstores.remove(dstore);
  }
}
