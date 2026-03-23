package dfs.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import java.util.*;

public class Rebalance {

  public static HashMap<Integer, ArrayList<String>> rebalance1(
      HashMap<Integer, ArrayList<String>> hashmap, int R, int N) {


    HashMap<Integer, ArrayList<String>> hashmap_rebalanced = new HashMap<>();
    // Step 2: Compute the total number of unique files in the hashmap
    Set<String> files = new HashSet<>();
    int total_files = 0;

    for (ArrayList<String> files_list : hashmap.values()) {
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
        ArrayList<String> files_list = hashmap_rebalanced.get(dstore);
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


  public static <K, V> K getKeyByValue(HashMap<K, V> map, V value) {
    for (Map.Entry<K, V> entry : map.entrySet()) {
      if (entry.getValue().equals(value)) {
        return entry.getKey();
      }
    }
    return null; // Value not found
  }

  public static void generateRebalanceInstructions(
      HashMap<Integer, ArrayList<String>> unbalanced,
      HashMap<Integer, ArrayList<String>> balanced
  ) {
    HashMap<Integer, ArrayList<String>> to_remove = new HashMap<>();
    HashMap<Integer, ArrayList<String>> to_add = new HashMap<>();
    ArrayList<SenderDstoreInfo> sender_dstores = new ArrayList<>();

    for (Integer dstore : unbalanced.keySet()) {
      ArrayList<String> unbalanced_files = unbalanced.get(dstore);
      ArrayList<String> balanced_files = balanced.get(dstore);

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
      ArrayList<String> unbalanced_files = unbalanced.get(dstore);

      Iterator<Map.Entry<Integer, ArrayList<String>>> iterator = temp.entrySet().iterator();
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

    HashMap<Integer, String> rebalanceInstructions = new HashMap<>();

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

  }

}
