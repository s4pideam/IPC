package org.betriebssysteme.Enum;

public enum EPackage {
    INIT((byte) 0x00),
    MAP((byte) 0x01),
    SHUFFLE((byte) 0x02),
    REDUCE((byte) 0x03),
    MERGE((byte) 0x04),
    CONNECTED((byte) 0x05),
    DONE((byte) 0x06);

    private final byte value;


    public final static String STRING_DELIMITER = "#";
    public final static int PACKET_SIZE_LENGTH = 8;
    private EPackage(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static EPackage fromByte(byte b) {
        for (EPackage ePackage : EPackage.values()) {
            if (ePackage.value == b) {
                return ePackage;
            }
        }
        throw new IllegalArgumentException("Invalid byte value: " + b);
    }
}
