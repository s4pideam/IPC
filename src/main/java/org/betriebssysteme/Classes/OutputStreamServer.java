package org.betriebssysteme.Classes;

import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Record.Offsets;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class OutputStreamServer extends IPCServer {

    protected boolean rapidFlush = false;
    protected int CHUNK_SIZE;
    protected List<List<Offsets>> offsets;


    public OutputStreamServer(String filePath) {
        super(filePath);
    }

    protected void generateInitMessage(DataOutputStream out, int index) {
        String result = index + EPackage.STRING_DELIMETER +
                alphabetSplit.stream()
                        .map(innerList -> String.join("", innerList))
                        .collect(Collectors.joining(EPackage.STRING_DELIMETER));
        this.send(out, EPackage.INIT, result);

    }

    @Override
    public void send(DataOutputStream out, EPackage header, String message) {
        try {
            synchronized (out) {
                switch (header) {
                    case INIT:
                    case MAP:
                    case REDUCE: {
                        out.writeByte(header.getValue());
                        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                        out.writeInt(bytes.length);
                        out.write(bytes);
                        if (header == EPackage.REDUCE) {
                            synchronized (this) {
                                ClientStatus clientStatus = this.clientStatus.get(out);
                                clientStatus.REDUCED++;
                                if ((clientStatus.REDUCED == this.CLIENT_NUMBERS - 1) && (clientStatus.MAPPED)
                                        && (!clientStatus.MERGED)) {
                                    out.writeByte(EPackage.MERGE.getValue());
                                }
                            }
                        }
                        if (header == EPackage.INIT || this.rapidFlush) {
                            out.flush();
                        }
                        break;
                    }
                    case SHUFFLE:
                    case MERGE:
                    case CONNECTED:
                    case DONE: {
                        out.writeByte(header.getValue());
                        if (rapidFlush) {
                            out.flush();
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Eigentlich unnötig hier zu synchronizieren. Und der zweite Fall tritt niemals ein (v+count)
    @Override
    public synchronized void updateWordCount(String word, int count) {
        wordCount.compute(word, (k, v) -> (v == null) ? count : v + count);
    }


}
