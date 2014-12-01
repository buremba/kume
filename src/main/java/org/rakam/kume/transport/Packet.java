package org.rakam.kume.transport;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/11/14 14:19.
*/
public class Packet {
    public final int packetNum;
    public Object data;
    public short service;

    public Packet(int packetNum, Object data, short service) {
        this.packetNum = packetNum;
        this.data = data;
        this.service = service;
    }

    public Packet(Object data, short service) {
        this.packetNum = -1;
        this.data = data;
        this.service = service;
    }

    public Object getData() {
        return data;
    }
    public Object getService() {
        return service;
    }

    @Override
    public String toString() {
        return "Packet{" +
                "packetNum=" + packetNum +
                ", data=" + data +
                '}';
    }
}
