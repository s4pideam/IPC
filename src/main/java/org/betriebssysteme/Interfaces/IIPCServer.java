package org.betriebssysteme.Interfaces;

import java.util.Map;

public interface IIPCServer {
    void start();
    void stop();
    void init(Map<String, Object> configMap);
}
