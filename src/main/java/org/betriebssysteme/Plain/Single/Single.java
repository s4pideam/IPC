package org.betriebssysteme.Plain.Single;

import org.betriebssysteme.Record.Offsets;
import org.betriebssysteme.Utils.Utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Single {
    private final RandomAccessFile randomAccessFile;
    private final HashMap<String,Integer> wordCount = new HashMap<>();
    private final int CHUNK_SIZE;


    public Single(String filePath, int CHUNK_SIZE) throws FileNotFoundException {
        this.randomAccessFile = new RandomAccessFile(filePath, "r");
        this.CHUNK_SIZE = CHUNK_SIZE;


    }

    public void count(){
        try{
            List<Offsets> offsets = getOffsets(this.CHUNK_SIZE);
            int currentOffset = 0;
            while (currentOffset < offsets.size()) {
                Offsets offset = offsets.get(currentOffset);
                byte[] packet = getChunk(offset.offset(), offset.length());
                String message = new String(packet);
                String[] words = message.toLowerCase().split("\\W+");
                for (String word : words) {
                    if (!word.matches("[a-z]+")) {
                        continue;
                    }
                    String key = String.valueOf(word.charAt(0));
                    wordCount.merge(word, 1, Integer::sum);
                }
                currentOffset++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        wordCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));

    }


    private  List<Offsets> getOffsets(int chunkSize) {
        List<Integer> offsets = new ArrayList<>();
        offsets.add(0);

        long fileSizeInBytes;
        try {
            fileSizeInBytes = this.randomAccessFile.length();
            int currentPosition = chunkSize;
            Pattern pattern = Pattern.compile("[a-zA-Z]");
            while (currentPosition < fileSizeInBytes) {
                this.randomAccessFile.seek(currentPosition);
                int ch = this.randomAccessFile.read();
                String character = Character.toString((char) ch);
                Matcher matcher = pattern.matcher(character);
                if (!matcher.matches()) {
                    offsets.add(++currentPosition);
                    currentPosition += chunkSize;
                } else {
                    currentPosition--;
                }
            }
            offsets.add((int) fileSizeInBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Offsets> outputOffsetList = new ArrayList<>();
        for (int i = 0; i < offsets.size()-1; i++) {
            int offset = offsets.get(i);
            int length = offsets.get(i + 1) - offset;
            outputOffsetList.add(new Offsets(offset, length));
        }
        return outputOffsetList;
    }



    public byte[] getChunk(int offset, int length) {
        synchronized (this.randomAccessFile) {
            byte[] chunk = new byte[length];
            try {
                this.randomAccessFile.seek(offset);
                this.randomAccessFile.read(chunk, 0, length);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return chunk;
        }
    }

}
