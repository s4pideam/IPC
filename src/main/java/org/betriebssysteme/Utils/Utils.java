package org.betriebssysteme.Utils;

import java.io.IOException;
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
                ProcessBuilder processBuilder = new ProcessBuilder("/bin/bash", "-c", "mkfifo " + path);
                Process process = processBuilder.start();

                // Wait for the process to complete
                int exitCode = process.waitFor();

                if (exitCode == 0) {
                    System.out.println("Named pipe created successfully");
                } else {
                    System.out.println("Error creating named pipe");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}