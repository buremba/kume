package org.rakam.kume.transport;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/11/14 14:19.
*/
public class Packet {
    public final int sequence;
    public Object data;
    public int service;

    public Packet(int sequence, Object data, int service) {
        this.sequence = sequence;
        this.data = data;
        this.service = service;
    }

    public Packet(Object data, short service) {
        this.sequence = -1;
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
                "sequence=" + sequence +
                ", data=" + data +
                '}';
    }
}
