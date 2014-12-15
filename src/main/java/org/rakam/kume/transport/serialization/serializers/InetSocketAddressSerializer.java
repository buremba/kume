package org.rakam.kume.transport.serialization.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.net.InetSocketAddress;

public class InetSocketAddressSerializer extends Serializer<InetSocketAddress> {

    @Override
    public void write(Kryo kryo, Output output, InetSocketAddress obj) {
        output.writeString(obj.getHostName());
        output.writeInt(obj.getPort(), true);
    }

    @Override
    public InetSocketAddress read(Kryo kryo, Input input, Class<InetSocketAddress> klass) {
        String host = input.readString();
        int port = input.readInt(true);
        return new InetSocketAddress(host, port);
    }
}
