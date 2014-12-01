package org.rakam.kume;

import com.esotericsoftware.kryo.KryoException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;
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
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf content = ((DatagramPacket) msg).content();
        try {
            Object o;
            try {
                o = cluster.getSerializer().toObject(content, content.readShort());
            } catch (KryoException e) {
                LOGGER.warn("Kryo couldn't deserialize packet", e);
                return;
            } catch (Exception e) {
                LOGGER.warn("Couldn't deserialize packet", e);
                return;
            }

            if (o instanceof Cluster.InternalRequest) {
                Cluster.InternalRequest req = ((Cluster.InternalRequest) o);
                Member sender = req.sender;
                if (sender == null || sender.equals(cluster.getLocalMember()))
                    return;

                req.run(cluster, null);
            }
//        } catch (Exception e) {
//            LOGGER.error("multicast server couldn't handle package. ", e);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if(cause instanceof ConnectException) {
            /* most likely the server is down but we don't do anything here
            since the heartbeat mechanism automatically removes the member from the cluster */
        }
    }
}
