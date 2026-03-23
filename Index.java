import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Index {

  private final Controller controller;
  private final ConcurrentHashMap<String, CountDownLatch> fileStoreAcks;
  private final ConcurrentHashMap<String, CountDownLatch> fileRemoveAcks;

  private final ConcurrentHashMap<String, String> fileStates;

  private final ConcurrentHashMap<String, Set<ControllerDstoreConnection>> index;
  private final ConcurrentHashMap<String, Integer> fileSizes;

  private final ReadWriteLock lock;
  public Index(Controller controller) {
    this.controller = controller;
    this.index = new ConcurrentHashMap<>();
    this.fileSizes = new ConcurrentHashMap<>();
    this.fileStoreAcks = new ConcurrentHashMap<>();
    this.fileRemoveAcks = new ConcurrentHashMap<>();
    this.fileStates = new ConcurrentHashMap<>();
    this.lock = new ReentrantReadWriteLock(true);
  }
  public Set<ControllerDstoreConnection> startToRemove(String file) {
    ControllerLogger.getInstance().startRemove(file);
    Set<ControllerDstoreConnection> dstores;
    try {
      this.lock.readLock().lock();
      dstores = this.index.get(file);
    } finally {
      this.lock.readLock().unlock();
    }

    if (dstores != null) {
      try {
        this.lock.writeLock().lock();
        this.index.put(file, Collections.emptySet());
        this.fileStates.put(file, "remove in progress");
      } finally {
        this.lock.writeLock().unlock();
        for (ControllerDstoreConnection dstore : dstores) {
          dstore.remove(file);
        }
        this.fileRemoveAcks.put(file, new CountDownLatch(dstores.size()));
      }
    }

    return dstores;
  }

  public void endRemove(String file) {
    try {
      this.lock.writeLock().lock();
      this.index.remove(file);
      this.fileSizes.remove(file);
      this.fileStates.put(file,"remove complete");
      this.fileStates.remove(file);
    } finally {
      this.lock.writeLock().unlock();
    }
  }
  public boolean startToStore(String file, Integer size) {
    ControllerLogger.getInstance().startStore(file, size);
    try {
      this.lock.readLock().lock();
      if (this.index.containsKey(file))
        return false;
    } finally {
      this.lock.readLock().unlock();
    }
    try {
      this.lock.writeLock().lock();
      this.index.put(file, Collections.emptySet());
      this.fileSizes.put(file, size);
      this.fileStates.put(file, "store in progress");
    } finally {
      this.lock.writeLock().unlock();
    }

    this.fileStoreAcks.put(file, new CountDownLatch(this.controller.getReplicationFactor()));
    return true;
  }

  public void endStoreOperation(String file, Set<ControllerDstoreConnection> dstores, boolean success) {
    ControllerLogger.getInstance().endStore(file, dstores, success);
    if (success) {
      try {
        this.lock.writeLock().lock();
        this.index.put(file, dstores);
        this.fileStates.put(file, "store complete");
      } finally {
        this.lock.writeLock().unlock();
        for (ControllerDstoreConnection dstore : dstores)
          dstore.store(file);
      }
    } else {
      try {
        this.lock.writeLock().lock();
        this.index.remove(file);
        this.fileSizes.remove(file);
        this.fileStates.remove(file);
      } finally {
        this.lock.writeLock().unlock();
      }
    }
  }

  public Set<ControllerDstoreConnection> getFileDstores(String file) {
    Set<ControllerDstoreConnection> dstore = null;

    try {
      this.lock.readLock().lock();
      if (this.index.get(file) != null) {
        dstore = Collections.synchronizedSet(new HashSet<>(this.index.get(file)));
      }
      return dstore;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public Integer getFileSize(String file) {
    return this.fileSizes.get(file);
  }

  public boolean awaitStore(String file) {
    try {
      return this.fileStoreAcks.get(file).await(this.controller.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      return false;
    }
  }

  public boolean awaitRemove(String file) {
    try {
      return this.fileRemoveAcks.get(file).await(this.controller.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      return false;
    }
  }

  public void addStoreAck(String file) {
    if (this.fileStoreAcks.containsKey(file)) {
      this.fileStoreAcks.get(file).countDown();
    }

  }


  public void addRemoveAck(String file) {
    if (this.fileRemoveAcks.containsKey(file))
      this.fileRemoveAcks.get(file).countDown();
  }

  public void removeControllerDstoreConnection(ControllerDstoreConnection dstoreConnection) {
    try {
      this.lock.writeLock().lock();
      this.index.forEach((key, value) -> value.removeIf(t -> t.getPort().equals(dstoreConnection.getPort())));
      Set<String> files = new HashSet<>(this.index.keySet());
      files.forEach(f -> {
        if (this.index.get(f).isEmpty()) {
          this.index.remove(f);
          this.fileSizes.remove(f);
        }
      });
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public Set<String> getFileSet() {
    try {
      this.lock.readLock().lock();
      return this.index.keySet();
    } finally {
      this.lock.readLock().unlock();
    }

  }
}
