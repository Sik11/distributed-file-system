package dfs.core;

public class SenderDstoreInfo {

  private final int senderDstore;
  private final String file;
  private final int destinationDstore;

  public SenderDstoreInfo(int senderDstore, String file, int destinationDstore) {
    this.senderDstore = senderDstore;
    this.file = file;
    this.destinationDstore = destinationDstore;
  }

  public int getSenderDstore() {
    return senderDstore;
  }

  public String getFile() {
    return file;
  }

  public int getDestinationDstore() {
    return destinationDstore;
  }

  @Override
  public String toString() {
    return "SenderDstoreInfo{" +
        "senderDstore=" + senderDstore +
        ", file='" + file + '\'' +
        ", destinationDstore=" + destinationDstore +
        '}';
  }
}
