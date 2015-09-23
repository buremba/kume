package org.rakam.kume.join.multicast;

import com.esotericsoftware.kryo.KryoException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;
import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.transport.MulticastOperationContext;
import org.rakam.kume.transport.serialization.Serializer;
import org.rakam.kume.transport.MulticastPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;

/**
* Created by buremba <Burak Emre Kabakcı> on 23/11/14 17:38.
*/
public class MulticastChannelAdapter extends ChannelInboundHandlerAdapter {
    final static Logger LOGGER = LoggerFactory.getLogger(MulticastChannelAdapter.class);

    private Cluster cluster;

    public MulticastChannelAdapter(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            DatagramPacket msg1 = (DatagramPacket) msg;
            ByteBuf content = msg1.content();
            Object o;
            try {
                o = Serializer.toObject(content, content.readShort());
            } catch (KryoException e) {
                LOGGER.warn("Kryo couldn't deserialize packet", e);
                return;
            } catch (Exception e) {
                LOGGER.warn("Couldn't deserialize packet", e);
                return;
            }

            if (o instanceof MulticastPacket) {
                MulticastPacket req = (MulticastPacket) o;
                Member sender = req.sender;
                if (sender == null || sender.equals(cluster.getLocalMember()))
                    return;
                req.data.run(cluster.getServices().get(0), new MulticastOperationContext(sender, 0));
            }else {
                LOGGER.warn("multicast server in member {}, couldn't handle package: {}", cluster.getLocalMember(), msg);
            }
        } catch (Exception e) {
            LOGGER.error("an error occurred while processing data in multicast server", e);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if(cause instanceof ConnectException) {
            /* most likely the server is down but we don't do anything here
            since the heartbeat mechanism automatically removes the member from the cluster */
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }
}
