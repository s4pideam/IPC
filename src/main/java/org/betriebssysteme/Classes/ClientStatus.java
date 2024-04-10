package org.betriebssysteme.Classes;

public class ClientStatus {
    public boolean MAPPED = false;
    public boolean SHUFFLED = false;
    public int REDUCED = 0;
    public boolean MERGED = false;
    public boolean FINISHED = false;

    @Override
    public String toString() {
        return "ClientStatus{" +
                "MAPPED=" + MAPPED +
                ", SHUFFLED=" + SHUFFLED +
                ", REDUCED=" + REDUCED +
                ", MERGED=" + MERGED +
                ", FINISHED=" + FINISHED +
                '}';
    }
}
