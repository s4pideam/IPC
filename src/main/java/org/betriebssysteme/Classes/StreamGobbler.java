package org.betriebssysteme.Classes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamGobbler extends Thread {
    InputStream is;
    String type;
    private double minLatency = Double.MAX_VALUE;
    private double maxLatency = Double.MIN_VALUE;

    public StreamGobbler(InputStream is, String type) {
        this.is = is;
        this.type = type;
    }

    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(type + "> " + line);
                if (line.contains("Minimale Latenz")) {
                    minLatency = Double.parseDouble(line.split(":")[1].trim().split(" ")[0]);
                } else if (line.contains("Maximale Latenz")) {
                    maxLatency = Double.parseDouble(line.split(":")[1].trim().split(" ")[0]);
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
