/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.kume;

import org.rakam.kume.network.ClientChannelAdapter;
import org.rakam.kume.network.TCPServerHandler;
import org.rakam.kume.service.Service;
import org.rakam.kume.transport.PacketDecoder;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.rakam.kume.transport.Packet;
import org.rakam.kume.transport.PacketEncoder;
import org.rakam.kume.util.ThrowableNioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/09/15 20:21.
 */
public class NettyTransport implements Transport {
    private final static Logger LOGGER = LoggerFactory.getLogger(NettyTransport.class);
    private final Cache<Integer, CompletableFuture<Object>> messageHandlers = CacheBuilder.newBuilder()
            .expireAfterWrite(200, TimeUnit.SECONDS)
            .removalListener(this::removalListener).build();

    // IO thread for TCP and UDP connections
    final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    // Processor thread pool that de-serializing/serializing incoming/outgoing packets
    final EventLoopGroup workerGroup = new NioEventLoopGroup(4);

    private final TCPServerHandler server;

    public NettyTransport(ThrowableNioEventLoopGroup requestExecutor, List<Service> services, Member localMember) {
        try {
            this.server = new TCPServerHandler(bossGroup, workerGroup, requestExecutor, services, localMember.getAddress());
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to bind TCP " + localMember.getAddress());
        }

    }

    private void removalListener(RemovalNotification<Integer, CompletableFuture<Object>> notification) {
        if (!notification.getCause().equals(RemovalCause.EXPLICIT))
            notification.getValue().completeExceptionally(new TimeoutException());
    }

    @Override
    public MemberChannel connect(Member member) throws InterruptedException {
        Bootstrap b = new Bootstrap();
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        b.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
                        p.addLast("packetDecoder", new PacketDecoder());
                        p.addLast("frameEncoder", new LengthFieldPrepender(4));
                        p.addLast("packetEncoder", new PacketEncoder());
                        p.addLast("server", new ClientChannelAdapter(messageHandlers));
                    }
                });


        ChannelFuture f = b.connect(member.getAddress()).sync()
                .addListener(future -> {
                    if (!future.isSuccess()) {
                        LOGGER.error("Failed to connect server {}", member.getAddress());
                    }
                }).sync();
        return new NettyChannel(f.channel());
    }

    @Override
    public void close() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();

        try {
            server.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initialize() {
        server.setAutoRead(true);
    }

//    @Override
//    public void pause() {
//        server.setAutoRead(false);
//    }
//
//    @Override
//    public void resume() {
//        server.setAutoRead(true);
//    }

    public class NettyChannel implements MemberChannel {
        private final Channel channel;

        public NettyChannel(Channel channel) {
            this.channel = channel;
        }

        public CompletableFuture ask(Packet message) {
            CompletableFuture future = new CompletableFuture<>();
            messageHandlers.put(message.sequence, future);
            channel.writeAndFlush(message);
            return future;
        }

        @Override
        public void send(Packet message) {
            channel.writeAndFlush(message);
        }

        @Override
        public void close() throws InterruptedException {
            channel.close().sync();
        }
    }

}
