package org.betriebssysteme.Interfaces;

import org.betriebssysteme.Enum.EPackage;

public interface ISendable<T> {
        void send(T outputstream, EPackage header,String message);
}
