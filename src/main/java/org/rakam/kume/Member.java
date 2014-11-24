package org.rakam.kume;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.NotNull;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.rakam.kume.transport.serialization.InetSocketAddressSerializer;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/11/14 16:37.
 */
public class Member {

    @NotNull
    @FieldSerializer.Bind(DefaultSerializers.StringSerializer.class)
    String id;

    @NotNull
    @FieldSerializer.Bind(DefaultSerializers.BooleanSerializer.class)
    boolean isMaster = false;

    @NotNull
    @FieldSerializer.Bind(InetSocketAddressSerializer.class)
    InetSocketAddress address;

    public InetSocketAddress getAddress() {
        return address;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Member)) return false;

        Member member = (Member) o;

        return id.equals(member.getId());
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public void master(boolean isMaster) {
        this.isMaster = isMaster;
    }

    public Member(InetSocketAddress address) {
        this.address = address;
        id = UUID.randomUUID().toString();
    }

    public Member(InetSocketAddress address, String id) {
        this.address = address;
        this.id = id;
    }

    public Member(String host, int port) {
        address = new InetSocketAddress(host, port);
        id = UUID.randomUUID().toString();
    }

//    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(id);
        output.writeBoolean(isMaster);
        kryo.writeObject(output, address);
    }

//    @Override
    public void read(Kryo kryo, Input input) {
        this.id = input.readString();
        this.isMaster = input.readBoolean();
        this.address = kryo.readObject(input, InetSocketAddress.class);
    }
}
