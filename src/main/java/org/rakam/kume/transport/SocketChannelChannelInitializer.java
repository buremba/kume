package org.rakam.kume.transport;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.rakam.kume.Cluster;
import org.rakam.kume.ServerChannelAdapter;

/**
* Created by buremba <Burak Emre Kabakcı> on 23/11/14 17:34.
*/
public class SocketChannelChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final Cluster eventBus;

    public SocketChannelChannelInitializer(Cluster eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
        p.addLast("packetDecoder", new PacketDecoder());
        p.addLast("frameEncoder", new LengthFieldPrepender(4));
        p.addLast("packetEncoder", new PacketEncoder());
        p.addLast("server", new ServerChannelAdapter(eventBus));
    }
}
