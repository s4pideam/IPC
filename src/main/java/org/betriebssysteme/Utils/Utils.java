package org.betriebssysteme.Utils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class Utils {
    public static void associateKeys(HashMap<String, HashMap<String, Integer>> hashMap, String keys,
            HashMap<String, Integer> object) {
        hashMap.put(keys, object);
        for (char key : keys.toCharArray()) {
            hashMap.put(String.valueOf(key), object);
        }
    }

    public static void countWords(HashMap<String, HashMap<String, Integer>> wordCount, String message) {
        String[] words = message.toLowerCase().split("\\W+");

        for (String word : words) {
            if (!word.matches("[a-z]+")) {
                continue;
            }
            String key = String.valueOf(word.charAt(0));
            wordCount.computeIfAbsent(key, k -> new HashMap<>());
            wordCount.get(key).merge(word, 1, Integer::sum);
        }
    }

    public static void createNamedPipe(Path path) {
        try {
            if (!Files.exists(path)) {
                // Create a named pipe using the mkfifo command
                File file = new File(String.valueOf(path));
                file.getParentFile().mkdirs();
                if (file.exists()){
                    file.delete();
                }
                Process proc = Runtime.getRuntime().exec("mkfifo " + path);
                int exitCode = proc.waitFor();
                if (exitCode == 0) {
                    System.out.println("Named pipe created successfully");
                } else {
                    System.out.println("Error creating named pipe");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}