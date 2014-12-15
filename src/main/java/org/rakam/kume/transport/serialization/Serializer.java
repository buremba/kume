package org.rakam.kume.transport.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/11/14 00:51.
 */
public class Serializer {

    public ByteBuf toByteBuf(Object obj) {
        Kryo kryo = KryoFactory.getKryoInstance();

        Output output = new Output(64, 4096);
        output.setPosition(2);
        kryo.writeClassAndObject(output, obj);
        int position = output.position();

        output.setPosition(0);
        output.writeShort(position-2);

        return Unpooled.copiedBuffer(output.getBuffer(), 0, position);
    }

    public <T> T toObject(ByteBuf obj, int size) {
        Kryo kryo = KryoFactory.getKryoInstance();

        byte[] bytes = new byte[size];
        obj.readBytes(bytes);
        return (T) kryo.readClassAndObject(new Input(bytes));
    }
}
