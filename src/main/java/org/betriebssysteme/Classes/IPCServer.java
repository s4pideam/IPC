package org.betriebssysteme.Classes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.betriebssysteme.Interfaces.IIPCServer;
import org.betriebssysteme.Interfaces.ITextTokenizer;
import org.betriebssysteme.Record.Offsets;

public class IPCServer implements IIPCServer, ITextTokenizer {
    private RandomAccessFile randomAccessFile = null;

    public IPCServer(String filePath) {
        try {
            this.randomAccessFile = new RandomAccessFile(filePath, "r");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            this.randomAccessFile.close();
            System.out.println("File closed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<List<String>> splitAlphabet(int clientNumbers) {
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        int totalElements = alphabet.length();
        int elementsPerSublist = totalElements / clientNumbers;
        int remainder = totalElements % clientNumbers;

        List<List<String>> outputSublists = new ArrayList<>();
        int index = 0;
        for (int i = 0; i < clientNumbers; i++) {
            List<String> sublist = new ArrayList<>();
            int sublistSize = elementsPerSublist + (i < remainder ? 1 : 0);
            for (int j = 0; j < sublistSize; j++) {
                sublist.add(alphabet.substring(index, index + 1));
                index++;
            }
            outputSublists.add(sublist);
        }
        return outputSublists;
    }

    private int[] getOffsets(int chunkSize) {
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

        return offsets.stream().mapToInt(Integer::intValue).toArray();
    }

    /*
     * private int[] getOffsets(int chunkSize) {
     * List<Integer> offsets = new ArrayList<>();
     * offsets.add(0);
     * 
     * long fileSizeInBytes;
     * try {
     * fileSizeInBytes = this.randomAccessFile.length();
     * int ch;
     * int currentPosition = chunkSize;
     * while (currentPosition < fileSizeInBytes) {
     * this.randomAccessFile.seek(currentPosition);
     * ch = this.randomAccessFile.read();
     * if (ch == ' ') {
     * offsets.add(++currentPosition);
     * currentPosition += chunkSize;
     * } else {
     * currentPosition--;
     * }
     * }
     * offsets.add((int) fileSizeInBytes);
     * } catch (IOException e) {
     * e.printStackTrace();
     * }
     * 
     * return offsets.stream().mapToInt(Integer::intValue).toArray();
     * }
     */
    @Override
    public List<List<Offsets>> getOffsets(int clientNumbers, int chunkSize) {
        int[] offsets = this.getOffsets(chunkSize);
        int totalElements = offsets.length;
        int elementsPerSublist = totalElements / clientNumbers;
        int remainder = Math.max((totalElements % clientNumbers) - 1, 0);

        List<List<Offsets>> outputOffsetList = new ArrayList<>();
        int index = 0;
        for (int i = 0; i < clientNumbers; i++) {
            List<Offsets> subList = new ArrayList<>();
            int sublistSize = Math.min(elementsPerSublist + (i < remainder ? 1 : 0), totalElements - index - 1);
            for (int j = 0; j < sublistSize; j++) {
                int offset = offsets[index];
                int length = offsets[index + 1] - offset;
                subList.add(new Offsets(offset, length));
                index++;
            }
            outputOffsetList.add(subList);

        }

        return outputOffsetList;
    }

    @Override
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

    @Override
    public void start() {
        throw new UnsupportedOperationException("Unimplemented method 'start'");
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException("Unimplemented method 'stop'");
    }

    @Override
    public void init(Map<String, Object> configMap) {
        throw new UnsupportedOperationException("Unimplemented method 'init'");
    }

}