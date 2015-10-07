package org.rakam.kume.transport;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.rakam.kume.Member;


public class MulticastPacket implements KryoSerializable {
    public Operation data;
    public Member sender;

    public MulticastPacket(Operation data, Member sender) {
        this.data = data;
        this.sender = sender;
    }
    public MulticastPacket() {
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeClassAndObject(output, data);
        kryo.writeObject(output, sender);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        data = (Operation) kryo.readClassAndObject(input);
        sender = kryo.readObject(input, Member.class);
    }
}
