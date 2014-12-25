package org.rakam.kume;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.NetUtil;
import org.rakam.kume.transport.MulticastPacket;
import org.rakam.kume.transport.serialization.Serializer;
import org.rakam.kume.util.NetworkUtil;

import java.net.InetSocketAddress;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/12/14 19:44.
 */
public class MulticastServerHandler {
    private final Member localMember;
    private NioDatagramChannel server;
    InetSocketAddress address;
    Bootstrap handler;

    public MulticastServerHandler(Cluster cluster, InetSocketAddress address) throws InterruptedException {
        this.address = address;

        handler = new Bootstrap()
                .channelFactory(() -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
                .localAddress(address)
                .group(new NioEventLoopGroup())
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.IP_MULTICAST_IF, NetworkUtil.getPublicInterface())
                .option(ChannelOption.AUTO_READ, false)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(NioDatagramChannel ch) throws Exception {
                        ch.pipeline().addLast(new MulticastChannelAdapter(cluster));
                    }
                });
        localMember = cluster.getLocalMember();
    }

    public MulticastServerHandler start() throws InterruptedException {
        server = (NioDatagramChannel) handler.bind(address.getPort()).sync().channel();
        server.joinGroup(address, NetworkUtil.getPublicInterface()).sync();

        return this;
    }

    public void setAutoRead(boolean bool) {
        server.config().setAutoRead(bool);
    }

    public void send(Operation req) {
        ByteBuf buf = Unpooled.wrappedBuffer(Serializer.toByteBuf(new MulticastPacket(req, localMember)));
        server.writeAndFlush(new DatagramPacket(buf, address, localMember.address));
    }

    public void close() throws InterruptedException {
        server.leaveGroup(address, NetUtil.LOOPBACK_IF).sync();
        server.close().sync();
    }
}
