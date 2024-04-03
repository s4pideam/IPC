package org.betriebssysteme.Utils;

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
            if (!word.matches("[a-z]+"))
                continue;
            String key = String.valueOf(word.charAt(0));
            wordCount.computeIfAbsent(key, k -> new HashMap<>());
            wordCount.get(key).merge(word, 1, Integer::sum);
        }
    }
}