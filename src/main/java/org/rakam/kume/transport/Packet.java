package org.rakam.kume.transport;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/11/14 14:19.
*/
public class Packet {
    public final int packetNum;
    public Object data;

    public Packet(int packetNum, Object data) {
        this.packetNum = packetNum;
        this.data = data;
    }

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return "Packet{" +
                "packetNum=" + packetNum +
                ", data=" + data +
                '}';
    }
}
