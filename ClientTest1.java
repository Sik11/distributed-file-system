import java.io.File;
import java.io.IOException;

public class ClientTest1 {
  public static void main(String[] args) throws Exception {

    final int cport = Integer.parseInt(args[0]);
    int timeout = Integer.parseInt(args[1]);

    // this client expects a 'downloads' folder in the current directory; all files loaded from the store will be stored in this folder
    File downloadFolder = new File("downloads");
    if (!downloadFolder.exists())
      if (!downloadFolder.mkdir())
        throw new RuntimeException("Cannot create download folder (folder absolute path: "
            + downloadFolder.getAbsolutePath() + ")");

    // this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
    File uploadFolder = new File("to_store");
    if (!uploadFolder.exists())
      throw new RuntimeException("to_store folder does not exist");

    // launch a single client
    testClient(cport, timeout, downloadFolder, uploadFolder);
  }

  public static void testClient(int cport, int timeout, File downloadFolder, File uploadFolder) {
    Client client = null;

    try {

      client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

      try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }

      try { list(client); } catch(IOException e) { e.printStackTrace(); }

      // store first file in the to_store folder twice, then store second file in the to_store folder once
      File fileList[] = uploadFolder.listFiles();
      if (fileList.length > 0) {
        try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
        try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
      }
      if (fileList.length > 1) {
        try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
      }

      String list[] = null;
      try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

      if (list != null)
        for (String filename : list)
          try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }

      if (list != null)
        for (String filename : list)
          try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
      if (list != null && list.length > 0)
        try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }

      try { list(client); } catch(IOException e) { e.printStackTrace(); }

    } finally {
      if (client != null)
        try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
    }
  }

  public static String[] list(Client client) throws IOException, NotEnoughDstoresException {
    System.out.println("Retrieving list of files...");
    String list[] = client.list();

    System.out.println("Ok, " + list.length + " files:");
    int i = 0;
    for (String filename : list)
      System.out.println("[" + i++ + "] " + filename);

    return list;
  }

}
