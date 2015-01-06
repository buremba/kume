package org.rakam.kume.transport;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.rakam.kume.ByteBufInput;
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
            int serviceId = buffer.readUnsignedShort();
            LOGGER.trace("Decoding Packet{sequence={}, service={}}", packetNum, serviceId);

            Object o;
            try {
                o = kryo.readClassAndObject(new ByteBufInput(buffer));
            } catch (KryoException e) {
                LOGGER.warn("Couldn't deserialize object", e);
                return;
            } catch (Exception e) {
                LOGGER.warn("Couldn't deserialize object", e);
                return;
            }

            Packet e = new Packet(packetNum, o, serviceId);
            out.add(e);
        } catch (Exception e) {
            LOGGER.error("error while handling package", e);
        }

    }
}
