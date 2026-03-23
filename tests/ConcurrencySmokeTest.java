import dfs.controller.Controller;
import dfs.dstore.Dstore;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConcurrencySmokeTest {

  public static void main(String[] args) throws Exception {
    int controllerPort = 19080;
    int timeoutMs = 4000;
    int rebalancePeriodSeconds = 120;
    int replicationFactor = 3;
    int clientCount = 6;
    int filesPerClient = 3;
    boolean startServers = true;

    if (args.length > 0 && args[0].equals("clients-only")) {
      startServers = false;
    }
    if (args.length > 1) {
      controllerPort = Integer.parseInt(args[1]);
    }
    final int resolvedControllerPort = controllerPort;

    Path root = Files.createTempDirectory("dfs-smoke-");
    Path dstore1 = root.resolve("dstore1");
    Path dstore2 = root.resolve("dstore2");
    Path dstore3 = root.resolve("dstore3");
    Files.createDirectories(dstore1);
    Files.createDirectories(dstore2);
    Files.createDirectories(dstore3);

    try {
      if (startServers) {
        new Controller(resolvedControllerPort, replicationFactor, timeoutMs, rebalancePeriodSeconds);
        new Dstore(19081, resolvedControllerPort, timeoutMs, dstore1.toString());
        new Dstore(19082, resolvedControllerPort, timeoutMs, dstore2.toString());
        new Dstore(19083, resolvedControllerPort, timeoutMs, dstore3.toString());
        Thread.sleep(2000);
      }

      Set<String> expectedFiles = ConcurrentHashMap.newKeySet();
      ExecutorService pool = Executors.newFixedThreadPool(clientCount);
      CountDownLatch startGate = new CountDownLatch(1);
      List<Future<?>> storeRuns = new ArrayList<>();

      for (int clientId = 0; clientId < clientCount; clientId++) {
        final int id = clientId;
        storeRuns.add(pool.submit(() -> {
          Client client = new Client(resolvedControllerPort, timeoutMs, Logger.LoggingType.ON_TERMINAL_ONLY);
          client.connect();
          try {
            startGate.await(5, TimeUnit.SECONDS);
            for (int i = 0; i < filesPerClient; i++) {
              String name = "client-" + id + "-file-" + i + ".txt";
              byte[] content = ("payload-" + id + "-" + i).getBytes(StandardCharsets.UTF_8);
              client.store(name, content);
              expectedFiles.add(name);

              byte[] loaded = client.load(name);
              if (!Arrays.equals(content, loaded)) {
                throw new IllegalStateException("Loaded content mismatch for " + name);
              }

              String[] listed = client.list();
              if (Arrays.stream(listed).noneMatch(name::equals)) {
                throw new IllegalStateException("List result missing stored file " + name);
              }
            }
            return null;
          } finally {
            client.disconnect();
          }
        }));
      }

      startGate.countDown();
      for (Future<?> run : storeRuns) {
        run.get(30, TimeUnit.SECONDS);
      }

      Client verifier = new Client(resolvedControllerPort, timeoutMs, Logger.LoggingType.ON_TERMINAL_ONLY);
      verifier.connect();
      try {
        String[] listed = verifier.list();
        Set<String> actualFiles = new TreeSet<>(Arrays.asList(listed));
        if (actualFiles.size() != expectedFiles.size() || !actualFiles.containsAll(expectedFiles)) {
          throw new IllegalStateException(
              "Unexpected files after concurrent stores. expected=" + expectedFiles.size()
                  + " actual=" + actualFiles.size());
        }

        for (String name : expectedFiles) {
          byte[] content = verifier.load(name);
          if (content.length == 0) {
            throw new IllegalStateException("Loaded empty content for " + name);
          }
        }
      } finally {
        verifier.disconnect();
      }

      List<String> removalOrder = new ArrayList<>(expectedFiles);
      CountDownLatch removeGate = new CountDownLatch(1);
      List<Future<?>> removeRuns = new ArrayList<>();
      for (int clientId = 0; clientId < clientCount; clientId++) {
        final int id = clientId;
        removeRuns.add(pool.submit(() -> {
          Client client = new Client(resolvedControllerPort, timeoutMs, Logger.LoggingType.ON_TERMINAL_ONLY);
          client.connect();
          try {
            removeGate.await(5, TimeUnit.SECONDS);
            for (int i = id; i < removalOrder.size(); i += clientCount) {
              client.remove(removalOrder.get(i));
            }
            return null;
          } finally {
            client.disconnect();
          }
        }));
      }

      removeGate.countDown();
      for (Future<?> run : removeRuns) {
        run.get(30, TimeUnit.SECONDS);
      }
      pool.shutdown();
      pool.awaitTermination(10, TimeUnit.SECONDS);

      Client finalVerifier = new Client(resolvedControllerPort, timeoutMs, Logger.LoggingType.ON_TERMINAL_ONLY);
      finalVerifier.connect();
      try {
        String[] remaining = finalVerifier.list();
        if (remaining.length != 0) {
          throw new IllegalStateException("Expected empty store after removals but found "
              + Arrays.toString(remaining));
        }
      } finally {
        finalVerifier.disconnect();
      }

      System.out.println("CONCURRENCY_SMOKE_TEST_PASS");
    } catch (Throwable t) {
      System.err.println("CONCURRENCY_SMOKE_TEST_FAIL");
      t.printStackTrace();
      System.exit(1);
    }
  }
}
