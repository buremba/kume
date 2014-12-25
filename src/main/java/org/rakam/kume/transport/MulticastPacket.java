package org.rakam.kume.transport;

import org.rakam.kume.Member;
import org.rakam.kume.Operation;


/**
 * Created by buremba <Burak Emre Kabakcı> on 25/12/14 20:16.
 */
public class MulticastPacket {
    public Operation data;
    public Member sender;

    public MulticastPacket(Operation data, Member sender) {
        this.data = data;
        this.sender = sender;
    }
}
