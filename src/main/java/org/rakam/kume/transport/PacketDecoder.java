package org.rakam.kume.transport;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.rakam.kume.transport.serialization.KryoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PacketDecoder extends ByteToMessageDecoder {
    private final Kryo kryo;
    final static Logger LOGGER = LoggerFactory.getLogger(PacketDecoder.class);

    public PacketDecoder() {
        this.kryo = KryoFactory.getKryoInstance();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        try {
            if (!buffer.isReadable())
                return;

            int packetNum = buffer.readInt();
            short serviceId = buffer.readShort();

            byte[] data = new byte[buffer.readableBytes()];
            buffer.readBytes(data);
            Object o;
            try {
                o = kryo.readClassAndObject(new Input(data));
            } catch (KryoException e) {
                LOGGER.warn("Couldn't deserialize object", e);
                return;
            } catch (Exception e) {
                LOGGER.warn("Couldn't deserialize object", e);
                return;
            }

            out.add(new Packet(packetNum, o, serviceId));
        } catch (Exception e) {
            LOGGER.error("error while handling package", e);
        }

    }
}
