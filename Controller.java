import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

class SenderDstoreInfo {

  private int senderDstore;
  private String file;
  private int destinationDstore;

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

public class Controller extends ServerSocket {

  private final Integer cport;
  private final Integer replicationFactor;
  private final Integer timeout;
  private final Integer rebalancePeriod;

  private final Index index;

  private final ReadWriteLock lock;

  private CountDownLatch rebalanceLatch;

  private ConcurrentHashMap<Integer,List<String>> dstoreList = new ConcurrentHashMap<>();



  private Set<ControllerDstoreConnection> dstoreConnections;
  private Set<ControllerClientConnection> clientConnections;



  public Controller (Integer cport, Integer replicationFactor, Integer timeout,
      Integer rebalancePeriod) throws Exception {
    super(cport);
    this.cport = cport;
    this.replicationFactor = replicationFactor;
    this.timeout = timeout;
    this.rebalancePeriod = rebalancePeriod;
    this.dstoreConnections = ConcurrentHashMap.newKeySet();
    this.clientConnections = ConcurrentHashMap.newKeySet();
    this.index = new Index(this);
    this.lock = new ReentrantReadWriteLock(true);
    ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, cport);
    new Thread(this::listen).start();
  }

  public void listen() {
    while (true) {
      try {
        onAccept(accept());
      } catch (Exception e) {
        ControllerLogger.getInstance().error("Cannot accept connection");
        throw new IllegalStateException("Cannot accept connection", e);
      }
    }
  }

  public void onAccept(Socket socket) {
    ControllerLogger.getInstance().connectionAccepted(socket);
    try {
      new Thread(() -> {
        try {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(socket.getInputStream()));
          String message;
          while ((message = reader.readLine()) != null) {
            String[] parts = message.split(" ");
            if (parts.length > 0) {
              if (parts[0].equals("JOIN")) {
                Integer port = Integer.parseInt(parts[1]);
                Socket dstore = new Socket(InetAddress.getLocalHost(), port);
                ControllerDstoreConnection dstoreConnection = new ControllerDstoreConnection(
                    Controller.this,socket, port, message);
                addDstoreConnection(dstoreConnection);
                startRebalance();
              } else {
                addClientConnection(new ControllerClientConnection(Controller.this, socket,
                    message));
              }
              break;
            }
          }
        } catch (Exception e) {
          ControllerLogger.getInstance().error("Error reading from socket");
        }
      }).start();
    } catch (Exception e) {
      ControllerLogger.getInstance().error(e.getMessage());
    }
  }

  public void addDstoreConnection(ControllerDstoreConnection dstoreConnection) {
    try {
      this.lock.writeLock().lock();
      dstoreConnections.add(dstoreConnection);
    } finally {
      this.lock.writeLock().unlock();
    }
    ControllerLogger.getInstance().addDstore(dstoreConnection.getPort());
    new Thread(dstoreConnection).start();
  }

  public void addClientConnection(ControllerClientConnection clientConnection) {
    clientConnections.add(clientConnection);
    ControllerLogger.getInstance().addClient(clientConnection.getPort());
    new Thread(clientConnection).start();
  }

  public void removeClientConnection(ControllerClientConnection clientConnection) {
    ControllerLogger.getInstance().removeClient(clientConnection.getPort());
    clientConnections.remove(clientConnection);
  }

  public void removeDstoreConnection(ControllerDstoreConnection dstoreConnection) {
    try {
      this.lock.writeLock().lock();
      ControllerLogger.getInstance().removeDstore(dstoreConnection.getPort());
      dstoreConnections.remove(dstoreConnection);
      this.index.removeControllerDstoreConnection(dstoreConnection);
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public void store(ControllerClientConnection client, String file_name, Integer size) {
    Set<ControllerDstoreConnection> dstoreConnectionSet;
    try {
      this.lock.readLock().lock();
      if (this.dstoreConnections.size() < replicationFactor) {
        client.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return;
      } else {
        dstoreConnectionSet = this.dstoreConnections.stream().sorted(
            Comparator.comparingInt(ControllerDstoreConnection::getNumberOfFiles)
        ).limit(this.replicationFactor).collect(Collectors.toSet());
      }
    } finally {
      this.lock.readLock().unlock();
    }

    if (!this.index.startToStore(file_name, size)) {
      client.send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
      return;
    }


    StringBuilder response = new StringBuilder(Protocol.STORE_TO_TOKEN);
    for (ControllerDstoreConnection d : dstoreConnectionSet)
      response.append(" ").append(d.getPort());

    client.send(response.toString());

    boolean complete = this.index.awaitStore(file_name);

    if (complete) {
      client.send(Protocol.STORE_COMPLETE_TOKEN);
    }

    synchronized (this) {
      this.index.endStoreOperation(file_name, dstoreConnectionSet, complete);
    }
  }
  public void load(ControllerClientConnection client, String file_name, Integer loadCount) {
    ControllerLogger.getInstance().startLoad(file_name);
    try {
      this.lock.readLock().lock();
      if (dstoreConnections.size() < this.replicationFactor) {
        client.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return;
      }
      if (!this.index.getFileSet().contains(file_name)) {
        client.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return;
      }
    } finally {
      this.lock.readLock().unlock();
    }

    List<ControllerDstoreConnection> dstoreConnectionList;
    synchronized (this) {
      dstoreConnectionList = this.index.getFileDstores(file_name).stream().toList();
    }
    if (dstoreConnectionList.isEmpty()) {
      client.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
    } else if (loadCount >= dstoreConnectionList.size()) {
      client.send(Protocol.ERROR_LOAD_TOKEN);
      ControllerLogger.getInstance().endLoad(file_name, false);
    } else {
      ControllerDstoreConnection dstoreConnection = dstoreConnectionList.get(loadCount);
      synchronized (client) {
        client.send(Protocol.LOAD_FROM_TOKEN + " " + dstoreConnection.getPort() + " " + this.index.getFileSize(file_name));
      }
      ControllerLogger.getInstance().endLoad(file_name, true);
    }
  }


  public void remove(ControllerClientConnection client, String file_name) {
    try {
      this.lock.readLock().lock();
      if (this.dstoreConnections.size() < this.replicationFactor) {
        client.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return;
      }
    } finally {
      this.lock.readLock().unlock();
    }

    Set<ControllerDstoreConnection> dstoreConnectionSet = this.index.startToRemove(file_name);
    if (dstoreConnectionSet == null || dstoreConnectionSet.isEmpty()) {
      client.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }


    for (ControllerDstoreConnection dstoreConnection : dstoreConnectionSet) {
      dstoreConnection.send(Protocol.REMOVE_TOKEN + " " + file_name);
    }

    boolean complete = this.index.awaitRemove(file_name);
    if (complete) {
      client.send(Protocol.REMOVE_COMPLETE_TOKEN);
      this.index.endRemove(file_name);
    }
    ControllerLogger.getInstance().endRemove(file_name, complete);

  }

  public void list(ControllerClientConnection clientConnection) {
    ControllerLogger.getInstance().startList();
    try {
      lock.readLock().lock();
      if (this.dstoreConnections.size() < this.replicationFactor) {
        clientConnection.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return;
      }
    } finally {
      lock.readLock().unlock();
    }
    clientConnection.send(Protocol.LIST_TOKEN + " " + String.join(" ", this.index.getFileSet()));
    ControllerLogger.getInstance().endList(this.index.getFileSet());
  }

  public void startRebalance() {

    try {
      this.lock.readLock().lock();
      if (this.dstoreConnections.size() < this.replicationFactor) {
        ControllerLogger.getInstance().log("Not enough Dstores to rebalance");
        return;
      }

      for (ControllerDstoreConnection dstoreConnection : this.dstoreConnections) {
        dstoreConnection.send(Protocol.LIST_TOKEN);
      }
    } finally {
      this.lock.readLock().unlock();
    }

    this.rebalanceLatch = new CountDownLatch(this.dstoreConnections.size());

    boolean success = awaitDstoreResponse();

    if (!success) {
      ControllerLogger.getInstance().log("Rebalance failed");
    } else {
      ControllerLogger.getInstance().log("All Dstores responded successfully");
      ConcurrentHashMap<Integer, List<String>> desired = rebalance(this.dstoreList,
          this.replicationFactor, this.dstoreConnections.size());
      ConcurrentHashMap<Integer, String> instructions = generateRebalanceInstructions(this.dstoreList,desired);
      for (Map.Entry<Integer, String> instruction : instructions.entrySet()) {
        ControllerDstoreConnection dstoreConnection = this.dstoreConnections.stream().toList().get(instruction.getKey());
        dstoreConnection.send(instruction.getValue());
      }

      ControllerLogger.getInstance().log("Rebalance desired: " + desired);
    }

    endRebalance();
  }

  public void endRebalance() {
    this.rebalanceLatch = null;
    this.dstoreList = new ConcurrentHashMap<>();
  }

  public boolean awaitDstoreResponse() {
    try {
      return this.rebalanceLatch.await(this.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      return false;
    }
  }

  public void addListAck(Integer port, List<String> files) {
    if (!this.dstoreList.containsKey(port)) {
      try {
        this.lock.writeLock().lock();
        if (!files.isEmpty()) {
          this.dstoreList.put(port, files);
        } else {
          this.dstoreList.put(port, new ArrayList<>());
        }
        this.rebalanceLatch.countDown();
      } finally {
        this.lock.writeLock().unlock();
      }
    }
  }

  public ConcurrentHashMap<Integer, List<String>> rebalance(
      ConcurrentHashMap<Integer, List<String>> hashmap, int R, int N) {


    ConcurrentHashMap<Integer, List<String>> hashmap_rebalanced = new ConcurrentHashMap<>();
    // Step 2: Compute the total number of unique files in the hashmap
    Set<String> files = new HashSet<>();
    int total_files = 0;

    for (List<String> files_list : hashmap.values()) {
      for (String file : files_list) {
        if (!files.contains(file)) {
          files.add(file);
          total_files++;
        }
      }
    }



    // Step 3: Calculate the target number of files per data store
    int files_per_dstore = (int) Math.ceil((double) (R * total_files) / N);
    int min_files_per_dstore = (int) Math.floor((double) (R * total_files) / N);

    boolean flag = false;
    for (Integer dstore : hashmap.keySet()) {
      if (hashmap.get(dstore).size() >=min_files_per_dstore && hashmap.get(dstore).size() <= files_per_dstore) {
        flag = true;
      } else {
        flag = false;
        break;
      }
    }
    if (flag) {
      return hashmap;
    }

    // Step 4: Initialize the dictionary to store the number of files assigned to each data store
    HashMap<Integer, Integer> dstore_files_count = new HashMap<>();
    for (Integer dstore : hashmap.keySet()) {
      dstore_files_count.put(dstore, 0);
    }

    // Step 5: Assign each unique file to R data stores
    for (String file_name : new HashSet<String>(hashmap.values().stream().flatMap(Collection::stream).toList())) {
      ArrayList<Integer> assigned_dstores = new ArrayList<>();
      // Step 6: Assign the file to R data stores with the fewest assigned files
      for (int i = 0; i < R; i++) {
        Integer min_dstore = Collections.min(dstore_files_count.entrySet(),
            Comparator.comparingInt(Entry::getValue)).getKey();
        if (!assigned_dstores.contains(min_dstore)) {
          assigned_dstores.add(min_dstore);
          dstore_files_count.put(min_dstore, dstore_files_count.get(min_dstore) + 1);
          hashmap_rebalanced.putIfAbsent(min_dstore, new ArrayList<String>());
          hashmap_rebalanced.get(min_dstore).add(file_name);
          // Step 7: Remove the data store from the list of candidate data stores if it has reached the target number of files
          if (dstore_files_count.get(min_dstore) >= files_per_dstore) {
            dstore_files_count.remove(min_dstore);
          }
        }
      }
    }

    // Step 8: Check if any data store has more files than the target number of files per data store and remove files from it
    for (Integer dstore : hashmap_rebalanced.keySet()) {
      int num_files = hashmap_rebalanced.get(dstore).size();
      if (num_files > files_per_dstore) {
        // Determine the number of files to remove
        int num_files_to_remove = num_files - files_per_dstore;
        // Iterate over the files in the data store and remove the required number of files
        List<String> files_list = hashmap_rebalanced.get(dstore);
        for (int i = 0; i < num_files_to_remove; i++) {
          String file_to_remove = files_list.get(0);
          files_list.remove(0);
          // Assign the file to the data store with the fewest assigned files
          ArrayList<Integer> assigned_dstores = new ArrayList<>();
          for (int j = 0; j < R; j++) {
            Integer min_dstore = Integer.valueOf(Collections.min(dstore_files_count.entrySet(),
                Comparator.comparingInt(Map.Entry::getValue)).getKey());
            if (!assigned_dstores.contains(min_dstore)) {
              assigned_dstores.add(min_dstore);
              dstore_files_count.put(min_dstore, dstore_files_count.get(min_dstore) + 1);
              hashmap_rebalanced.putIfAbsent(min_dstore, new ArrayList<String>());
              hashmap_rebalanced.get(min_dstore).add(file_to_remove);
            }
          }
        }
      }
    }

    // Step 9: Return the rebalanced hashmap
    return hashmap_rebalanced;
  }

  public Integer getCport() {
    return cport;
  }

  public Integer getReplicationFactor() {
    return replicationFactor;
  }

  public Integer getTimeout() {
    return timeout;
  }

  public Integer getRebalancePeriod() {
    return rebalancePeriod;
  }

  public Index getIndex() {
    return index;
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Error: invalid number of arguments");
      return;
    }

    Integer cport;
    Integer replicationFactor;
    Integer timeout;
    Integer rebalancePeriod;
    try {
      cport = Integer.parseInt(args[0]);
      replicationFactor = Integer.parseInt(args[1]);
      timeout = Integer.parseInt(args[2]);
      rebalancePeriod = Integer.parseInt(args[3]);
      try {
        Controller controller = new Controller(cport, replicationFactor, timeout, rebalancePeriod);
      } catch (Exception e) {
        System.err.println("Error: cannot create controller");
      }
    } catch (NumberFormatException e) {
      System.err.println("Error: Invalid arguments");
    }
  }

  public static <K, V> K getKeyByValue(HashMap<K, V> map, V value) {
    for (Map.Entry<K, V> entry : map.entrySet()) {
      if (entry.getValue().equals(value)) {
        return entry.getKey();
      }
    }
    return null; // Value not found
  }

  public static ConcurrentHashMap<Integer, String> generateRebalanceInstructions(
      ConcurrentHashMap<Integer, List<String>> unbalanced,
      ConcurrentHashMap<Integer, List<String>> balanced
  ) {
    HashMap<Integer, ArrayList<String>> to_remove = new HashMap<>();
    HashMap<Integer, ArrayList<String>> to_add = new HashMap<>();
    ArrayList<SenderDstoreInfo> sender_dstores = new ArrayList<>();

    for (Integer dstore : unbalanced.keySet()) {
      List<String> unbalanced_files = unbalanced.get(dstore);
      List<String> balanced_files = balanced.get(dstore);

      ArrayList<String> filesToRemove = new ArrayList<>(unbalanced_files);
      filesToRemove.removeAll(balanced_files);
      if (!filesToRemove.isEmpty()) {
        to_remove.put(dstore, filesToRemove);
      }

      ArrayList<String> filesToAdd = new ArrayList<>(balanced_files);
      filesToAdd.removeAll(unbalanced_files);
      if (!filesToAdd.isEmpty()) {
        to_add.put(dstore, filesToAdd);
      }
    }

    HashMap<Integer, ArrayList<String>> temp = new HashMap<>(to_add);
    for (Integer dstore : unbalanced.keySet()) {
      List<String> unbalanced_files = unbalanced.get(dstore);

      Iterator<Entry<Integer, ArrayList<String>>> iterator = temp.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, ArrayList<String>> entry = iterator.next();
        Integer dstore2 = entry.getKey();
        ArrayList<String> to_add_files = entry.getValue();

        ArrayList<String> intersectFiles = new ArrayList<>(to_add_files);
        intersectFiles.retainAll(unbalanced_files);
        if (!intersectFiles.isEmpty()) {
          for (String file : intersectFiles) {
            sender_dstores.add(new SenderDstoreInfo(dstore, file, dstore2));
          }
          iterator.remove(); // Safely remove the entry from temp
        }
      }
    }

    ConcurrentHashMap<Integer, String> rebalanceInstructions = new ConcurrentHashMap<>();

    for (Integer dstore : unbalanced.keySet()) {
      if (!to_add.containsKey(dstore) && !to_remove.containsKey(dstore)) {
        String instruction = "REBALANCE 0 0";
        rebalanceInstructions.put(dstore, instruction);
      } else {
        String to_remove_string;
        if (to_remove.containsKey(dstore)) {
          ArrayList<String> filesToRemove = to_remove.get(dstore);
          int num_files_to_remove = filesToRemove.size();
          StringBuilder files_to_remove_sb = new StringBuilder();
          for (String file : filesToRemove) {
            files_to_remove_sb.append(" ").append(file);
          }
          to_remove_string = num_files_to_remove + files_to_remove_sb.toString();
        } else {
          to_remove_string = "0";
        }

        HashMap<String, List<Integer>> files_to_send_dstores = new HashMap<>();
        if (sender_dstores.size() > 0) {
          for (SenderDstoreInfo senderDstoreInfo : sender_dstores) {
            if (senderDstoreInfo.getSenderDstore() == dstore) {
              files_to_send_dstores.putIfAbsent(senderDstoreInfo.getFile(), new ArrayList<>());
              files_to_send_dstores.get(senderDstoreInfo.getFile()).add(senderDstoreInfo.getDestinationDstore());
            }
          }
        }

        String num_files_to_send;
        StringBuilder files_to_send_sb = new StringBuilder();
        if (files_to_send_dstores.isEmpty()) {
          num_files_to_send = "0";
        } else {
          num_files_to_send = String.valueOf(files_to_send_dstores.size());
          for (Map.Entry<String, List<Integer>> entry : files_to_send_dstores.entrySet()) {
            String file = entry.getKey();
            List<Integer> dstores = entry.getValue();
            int num_dstores = dstores.size();
            StringBuilder dstores_sb = new StringBuilder();
            for (Integer d : dstores) {
              dstores_sb.append(" ").append(d);
            }
            files_to_send_sb.append(" ").append(file).append(" ").append(num_dstores).append(dstores_sb);
          }
        }

        String instruction = "REBALANCE " + num_files_to_send + files_to_send_sb + " " + to_remove_string;
        rebalanceInstructions.put(dstore, instruction);
      }
    }

    return rebalanceInstructions;
  }

}
