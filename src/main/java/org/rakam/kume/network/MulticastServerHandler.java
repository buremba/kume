package org.rakam.kume.network;

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
import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.transport.Operation;
import org.rakam.kume.transport.MulticastPacket;
import org.rakam.kume.transport.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.NetworkInterface;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/12/14 19:44.
 */
public class MulticastServerHandler {
    final static Logger LOGGER = LoggerFactory.getLogger(MulticastServerHandler.class);

    private final Member localMember;
    private NioDatagramChannel server;
    InetSocketAddress address;
    Bootstrap handler;
    static NetworkInterface multicastInterface = NetUtil.LOOPBACK_IF;
    private boolean joinGroup;

    public MulticastServerHandler(Cluster cluster, InetSocketAddress address) throws InterruptedException {
        this.address = address;

        handler = new Bootstrap()
                .channelFactory(() -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
                .localAddress(address)
                .group(new NioEventLoopGroup())
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.IP_MULTICAST_IF, multicastInterface)
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
        server.joinGroup(address, multicastInterface).sync();
        // why netty doesn't have a get method for group memberships?
        joinGroup = true;
        return this;
    }

    public void setAutoRead(boolean bool) {
        server.config().setAutoRead(bool);
    }

    public void sendMulticast(Operation req) {
        ByteBuf buf = Unpooled.wrappedBuffer(Serializer.toByteBuf(new MulticastPacket(req, localMember)));
        server.writeAndFlush(new DatagramPacket(buf, address, localMember.getAddress()));
    }
    public void send(InetSocketAddress address, Operation<Cluster.InternalService> req) {
        ByteBuf buf = Unpooled.wrappedBuffer(Serializer.toByteBuf(new MulticastPacket(req, localMember)));
        server.writeAndFlush(new DatagramPacket(buf, address, localMember.getAddress()));
    }

    public void close() throws InterruptedException {
        server.leaveGroup(address, NetUtil.LOOPBACK_IF).sync();
        server.close().sync();
    }

    public void setJoinGroup(boolean joinGroup) {
        try {
            if(this.joinGroup && !joinGroup)
                server.leaveGroup(address, multicastInterface).sync();
            else
            if(!this.joinGroup && joinGroup)
                server.joinGroup(address, multicastInterface).sync();
        } catch (InterruptedException e) {
            LOGGER.error("couldn't change multicast server state.", e);
        }

        this.joinGroup = joinGroup;
    }

}
