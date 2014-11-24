package org.rakam.kume.transport;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.rakam.kume.Cluster;
import org.rakam.kume.Member;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/11/14 00:51.
 */
public class Serializer {
    private final Kryo kryo = new Kryo();

    public Serializer() {
        kryo.register(Member.class);
        kryo.register(Cluster.AddMemberRequest.class);
        kryo.register(Cluster.HeartbeatOperation.class);
    }

    public ByteBuf toByteBuf(Object obj) {
        Output output = new Output(64, 4096);
        output.setPosition(2);
        kryo.writeClassAndObject(output, obj);
        int position = output.position();

        output.setPosition(0);
        output.writeShort(position-2);

        return Unpooled.copiedBuffer(output.getBuffer(), 0, position);
    }

    public <T> T toObject(ByteBuf obj, int size) {
        byte[] bytes = new byte[size];
        obj.readBytes(bytes);
        try {
            return (T) kryo.readClassAndObject(new Input(bytes));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public <T> T toObject(ByteBuf obj, Class<T> clazz) {
        return kryo.readObject(new Input(obj.array()), clazz);
    }
}
