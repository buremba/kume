package org.rakam.kume;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.NetUtil;
import org.rakam.kume.service.Service;
import org.rakam.kume.transport.Packet;
import org.rakam.kume.transport.PacketDecoder;
import org.rakam.kume.transport.PacketEncoder;
import org.rakam.kume.transport.serialization.Serializer;
import org.rakam.kume.util.NetworkUtil;
import org.rakam.kume.util.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/11/14 21:41.
 */
public class Cluster implements Service, ClusterMBean {
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    final EventLoopGroup workerGroup = new NioEventLoopGroup();

    final private List<Service> services;
    final private AtomicInteger messageSequence = new AtomicInteger();

    final private ConcurrentHashMap<Member, Channel> clusterConnection = new ConcurrentHashMap<>();

    final Cache<Integer, CompletableFuture<Result>> messageHandlers = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS).removalListener(new RemovalListener<Integer, CompletableFuture<Result>>() {
                @Override
                public void onRemoval(RemovalNotification<Integer, CompletableFuture<Result>> notification) {
                    if(!notification.getCause().equals(RemovalCause.EXPLICIT))
                        notification.getValue().complete(Result.FAILED);
                }
            }).build();
    final private Member localMember;
    final private NioDatagramChannel multicastServer;
    private Member master;
    final private Channel server;
    final private InetSocketAddress multicastAddress;
    final private List<MembershipListener> membershipListeners = Collections.synchronizedList(new ArrayList<>());

    final private Serializer serializer = new Serializer();

    final private Map<Member, Long> heartbeatMap = new ConcurrentHashMap<>();
    final private long clusterStartTime;
    final private ScheduledFuture<?> heartbeatTask;

    public Cluster(Collection<Member> cluster, ServiceInitializer serviceGenerators, InetSocketAddress serverAddress) throws InterruptedException {
        for (Member member : cluster) {
            // lazily create clients for fast startup
            clusterConnection.put(member, null);
        };
        clusterStartTime = System.currentTimeMillis();

        CompletableFuture<Void> startFuture = new CompletableFuture<>();

        ChannelFuture bind = new ServerBootstrap()
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
                p.addLast("packetDecoder", new PacketDecoder());
                p.addLast("frameEncoder", new LengthFieldPrepender(4));
                p.addLast("packetEncoder", new PacketEncoder());
                startFuture.thenAccept(x -> p.addLast("server", new ServerChannelAdapter(Cluster.this)));
            }
        }).bind(serverAddress);

        server = bind.sync()
                .addListener(future -> {
                    if (!future.isSuccess()) {
                        LOGGER.error("Failed to bind {}", bind.channel().localAddress());
                    }
                }).channel();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    server.closeFuture().sync();
                    System.out.println("wtf");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        localMember = new Member((InetSocketAddress) server.localAddress());
        master = localMember;

        multicastAddress = new InetSocketAddress("239.255.27.1", 14878);

        EventLoopGroup group = new NioEventLoopGroup();


        Bootstrap a = new Bootstrap()
                .group(group)
                .channelFactory(() -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
                .localAddress(multicastAddress.getHostName(), multicastAddress.getPort())
                .option(ChannelOption.IP_MULTICAST_IF, NetUtil.LOOPBACK_IF)
                .option(ChannelOption.SO_REUSEADDR, true)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(NioDatagramChannel ch) throws Exception {
                        startFuture.thenAccept(x -> ch.pipeline().addLast(new MulticastChannelAdapter(Cluster.this)));
                    }
                });

        multicastServer = (NioDatagramChannel) a.bind(multicastAddress.getPort()).sync().channel();
        multicastServer.joinGroup(multicastAddress, NetUtil.LOOPBACK_IF).sync();

        ByteBuf heartbeatBuf = Unpooled.unreleasableBuffer(serializer.toByteBuf(new HeartbeatOperation(localMember)));

        multicastServer.writeAndFlush(new DatagramPacket(heartbeatBuf, multicastAddress));

        heartbeatTask = workerGroup.scheduleAtFixedRate(() -> {
            long time = System.currentTimeMillis();
            heartbeatMap.forEach((member, lastResponse) -> {
                if (time - lastResponse > 2000) {
//                    removeMember(member);
                }
            });

            multicastServer.writeAndFlush(new DatagramPacket(heartbeatBuf, multicastAddress, localMember.address));
        }, 500, 500, TimeUnit.MILLISECONDS);

        services = IntStream.range(0, serviceGenerators.size())
                .mapToObj(idx -> serviceGenerators.get(idx).constructor.newInstance(new ServiceContext((short) idx)))
                .collect(Collectors.toList());

        startFuture.complete(null);
        LOGGER.info("{} started listening on {}, listening UDP multicast server {}", localMember, server.localAddress(), multicastAddress);
    }

    public Cluster(Collection<Member> cluster, ServiceInitializer serviceGenerators) throws InterruptedException {
        this(cluster, serviceGenerators, new InetSocketAddress(NetworkUtil.getDefaultAddress(), 0));
    }

    public void addMember(Member member) {
        if (!clusterConnection.contains(member)) {
            LOGGER.info("Discovered new member {}", member);

            Channel channel;
            try {
                channel = connectServer(member.getAddress());
            } catch (InterruptedException e) {
                return;
            }
            clusterConnection.put(member, channel);
            heartbeatMap.put(member, System.currentTimeMillis());
            membershipListeners.forEach(x -> Throwables.propagate(() -> x.memberAdded(member)));
        }
    }
    public void removeMember(Member member) {
        clusterConnection.remove(member);
        LOGGER.info("Member removed {}", member);
        heartbeatMap.remove(member);
        membershipListeners.forEach(l -> Throwables.propagate(() -> l.memberRemoved(member)));
    }

    private Channel connectServer(SocketAddress serverAddr) throws InterruptedException {
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


        ChannelFuture f = b.connect(serverAddr).sync()
                .addListener(future -> {
                    if (!future.isSuccess()) {
                        LOGGER.error("Failed to connect server {}", serverAddr);
                    }
                }).sync();
        return f.channel();
    }

    public void addMembershipListener(MembershipListener listener) {
        membershipListeners.add(listener);
    }

    public Set<Member> getMembers() {
        Set<Member> members = new HashSet<>(clusterConnection.keySet());
        members.add(localMember);
        return Collections.unmodifiableSet(members);
    }

    public <T extends Service> T getService(Class<T> serviceClass) {
        return (T) services.stream().filter(service -> service.getClass().equals(serviceClass)).findAny().get();
    }


    public Member getLocalMember() {
        return localMember;
    }

    private void send(Member server, Object bytes, short service) {
        sendInternal(getConnection(server), bytes, service);
    }

    private void send(Member server, Request request, short service) {
        sendInternal(getConnection(server), request, service);
    }

    public void sendAllMembersInternal(Object bytes, short service) {
        clusterConnection.forEach((member, conn) -> {
            if(!member.equals(localMember)) {
                LOGGER.debug("member {} ", member);
                sendInternal(conn, bytes, service);
            }
        });
    }

    public Map<Member, CompletableFuture<Result>> askAllMembersInternal(Object bytes, short service) {
        Map<Member, CompletableFuture<Result>> map = new ConcurrentHashMap<>();
        clusterConnection.forEach((member, conn) -> {
            if(!member.equals(localMember)) {
                LOGGER.debug("member {} ", member);
                map.put(member, askInternal(conn, bytes, service));
            }
        });
        return map;
    }

    public void close() throws InterruptedException {
        for (Channel entry : clusterConnection.values()) {
            entry.close().sync();
        }
        multicastServer.leaveGroup(multicastAddress, NetUtil.LOOPBACK_IF).sync();
        multicastServer.close().sync();
        server.close().sync();
        heartbeatTask.cancel(true);
        services.forEach(s -> s.onClose());
        workerGroup.shutdownGracefully().await();
    }

    public void sendInternal(Channel channel, Object obj, short service) {
        Packet message = new Packet(obj, service);
        channel.writeAndFlush(message);
    }

    public CompletableFuture<Result> askInternal(Channel channel, Object obj, short service) {
        CompletableFuture<Result> future = new CompletableFuture<>();

        int andIncrement = messageSequence.getAndIncrement();
        Packet message = new Packet(andIncrement, obj, service);
        messageHandlers.put(andIncrement, future);

        channel.writeAndFlush(message);
        return future;
    }

    private Channel getConnection(Member member) {
        Channel channel = clusterConnection.get(member);
        if (channel == null) {
            Channel created;
            try {
                created = connectServer(member.address);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
            clusterConnection.put(member, created);
            return created;
        }
        return channel;
    }

    public boolean isMaster() {
        return localMember.equals(master);
    }

    public Member getMaster() {
        return master;
    }

    public List<Service> getServices() {
        return Collections.unmodifiableList(services);
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public static class HeartbeatOperation extends InternalRequest{
        public HeartbeatOperation(Member me) {
            sender = me;
        }

        @Override
        public void run(Cluster cluster, OperationContext ctx) {
            if(cluster.heartbeatMap.containsKey(sender)) {
                cluster.heartbeatMap.put(sender, System.currentTimeMillis());
            } else {
                cluster.addMember(sender);
            }
        }
    }


    public static class AddMemberRequest extends InternalRequest {
        Member member;

        public AddMemberRequest(Member member, Member sender) {
            this.member = member;
            this.sender = sender;
        }

        public void run(Cluster cluster, OperationContext ctx) {


//            if (cluster.isMaster()) {
//                AtomicInteger positive = new AtomicInteger();
//                AtomicInteger negative = new AtomicInteger();
//                final int quorum = cluster.clusterMembers.size() / 2;
//
//                if (quorum == 0) {
//                    cluster.clusterMembers.add(member);
//                    cluster.membershipListeners.forEach(x -> x.memberAdded(member));
//
//                    if (member.getId().compareTo(cluster.localMember.getId()) > 0) {
//                        cluster.master = member;
//                    } else {
//                        cluster.master = cluster.localMember;
//                    }
//                }
//                cluster.sendAllMembers(new AddMemberRequest(member, cluster.localMember)).values()
//                        .forEach(future -> future.thenAccept(result -> {
//                            if (result.isSucceeded()) {
//                                if (result.data.equals(Boolean.TRUE))
//                                    positive.incrementAndGet();
//                                else
//                                    negative.incrementAndGet();
//                            }
//
//                            if (positive.get() > quorum) {
//                                System.out.println("ok");
//                            } else if (negative.get() > quorum) {
//                                System.out.println("yok");
//                            }
//                        }));
//            }
        }
    }


    public static abstract class InternalOperation implements Operation {
        public Member sender;

        @Override
        public int getService() {
            return -1;
        }
    }

    public static abstract class InternalRequest implements Request<Cluster> {
        public Member sender;
    }

    public void pause() {
        server.config().setAutoRead(false);
    }

    public void resume() {
        server.config().setAutoRead(true);
    }

    @Override
    public void handle(OperationContext ctx, Object request) {

    }

    public class ServiceContext<T extends Service> {
        short service;

        public ServiceContext(short service) {
            this.service = service;
        }

        public void send(Member server, Object bytes) {
            if(server.equals(localMember)) {
                LocalOperationContext ctx1 = new LocalOperationContext(null);
                // move to an executor handler
                services.get(service).handle(ctx1, bytes);
            }else {
                sendInternal(getConnection(server), bytes, service);
            }
        }

        public void send(Member server, Request<T> request) {
            sendInternal(getConnection(server), request, service);
        }

        public void sendAllMembers(Object bytes) {
            sendAllMembersInternal(bytes, service);
        }

        public void sendAllMembers(Request<T> bytes) {
            sendAllMembersInternal(bytes, service);
        }

        public CompletableFuture<Result> ask(Member server, Object bytes) {
            if(server.equals(localMember)) {
                CompletableFuture<Result> future = new CompletableFuture<>();
                LocalOperationContext ctx1 = new LocalOperationContext(future);
                // move to an executor handler
                services.get(service).handle(ctx1, bytes);
                return future;
            }else {
                return askInternal(getConnection(server), bytes, service);
            }
        }

        public CompletableFuture<Result> ask(Member server, Request<T> request) {
            if(server.equals(localMember)) {
                CompletableFuture<Result> future = new CompletableFuture<>();
                LocalOperationContext ctx1 = new LocalOperationContext(future);
                request.run((T) services.get(service), ctx1);
                return future;
            }else {
                return askInternal(getConnection(server), request, service);
            }
        }

        public Map<Member, CompletableFuture<Result>> askAllMembers(Object bytes) {
            return askAllMembersInternal(bytes, service);
        }

        public Map<Member, CompletableFuture<Result>> askAllMembers(Request<T> bytes) {
            return askAllMembersInternal(bytes, service);
        }

        public Cluster getCluster() {
            return Cluster.this;
        }

        public long startTime() {
            return clusterStartTime;
        }
    }
}
