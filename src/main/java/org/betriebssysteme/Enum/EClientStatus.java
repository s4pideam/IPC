package org.betriebssysteme.Enum;

public enum EClientStatus {
    WORKING((byte) 0x00),
    DONE((byte) 0x02);

    private final byte value;
    
    private EClientStatus(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static EClientStatus fromByte(byte b) {
        for (EClientStatus eClientStatus : EClientStatus.values()) {
            if (eClientStatus.value == b) {
                return eClientStatus;
            }
        }
        throw new IllegalArgumentException("Invalid byte value: " + b);
    }
}
