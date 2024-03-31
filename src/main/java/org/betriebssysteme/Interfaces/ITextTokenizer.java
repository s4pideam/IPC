package org.betriebssysteme.Interfaces;

import java.util.List;

import org.betriebssysteme.Record.Offsets;

public interface ITextTokenizer {
    List<List<Offsets>> getOffsets(int clientNumbers, int chunkSize);
    byte[] getChunk(int offset, int length);
}
