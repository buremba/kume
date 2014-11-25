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
import org.rakam.kume.transport.Serializer;
import org.rakam.kume.transport.SocketChannelChannelInitializer;
import org.rakam.kume.util.NetworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/11/14 21:41.
 */
public class Cluster implements Service {
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    EventLoopGroup workerGroup = new NioEventLoopGroup();

    private List<Service> services;
    private AtomicInteger messageSequence = new AtomicInteger();

    private final ConcurrentHashMap<String, Channel> clusterConnection = new ConcurrentHashMap<>();
    private final ArrayList<Member> clusterMembers;

    static final Cache<Integer, CompletableFuture<Result>> messageHandlers = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS).removalListener(new RemovalListener<Integer, CompletableFuture<Result>>() {
                @Override
                public void onRemoval(RemovalNotification<Integer, CompletableFuture<Result>> notification) {
                    if(!notification.getCause().equals(RemovalCause.EXPLICIT))
                        notification.getValue().complete(Result.FAILED);
                }
            }).build();
    private Member localMember;
    private NioDatagramChannel multicastServer;
    private Member master;
    private Channel server;
    private InetSocketAddress multicastAddress;
    private List<MembershipListener> membershipListeners = new ArrayList<>();

    private Serializer serializer = new Serializer();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private Map<Member, Long> heartbeatMap = new ConcurrentHashMap<>();
    private long clusterStartTimestamp;

    public Cluster(Collection<Member> cluster, ServiceInitializer serviceGenerators, InetSocketAddress serverAddress) throws InterruptedException {
        clusterMembers = new ArrayList<>(cluster);
        start(serviceGenerators, serverAddress);
    }

    public Cluster(Collection<Member> cluster, ServiceInitializer serviceGenerators) throws InterruptedException {
        this(cluster, serviceGenerators, new InetSocketAddress(NetworkUtil.getDefaultAddress(), 0));
    }

    private void start(ServiceInitializer serviceGenerators, InetSocketAddress addr) throws InterruptedException {
        startServer(addr);

        localMember = new Member((InetSocketAddress) server.localAddress());
        clusterMembers.add(localMember);

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
                        ch.pipeline().addLast(new MulticastChannelAdapter(Cluster.this));
                    }
                });



        multicastServer = (NioDatagramChannel) a.bind(multicastAddress.getPort()).sync().channel();
        multicastServer.joinGroup(multicastAddress, NetUtil.LOOPBACK_IF).sync();

        ByteBuf byteBuf = serializer.toByteBuf(new AddMemberRequest(localMember, localMember));
        multicastServer.writeAndFlush(new DatagramPacket(byteBuf, multicastAddress));

        clusterStartTimestamp = System.currentTimeMillis();
        clusterMembers.stream().filter(m -> !m.equals(localMember))
                .forEach(member -> heartbeatMap.put(member, clusterStartTimestamp));

        ByteBuf heatbeatBuf = Unpooled.unreleasableBuffer(serializer.toByteBuf(new HeartbeatOperation(localMember)));
        multicastServer.writeAndFlush(new DatagramPacket(heatbeatBuf, multicastAddress));

        scheduler.scheduleAtFixedRate(() -> {
            long time = System.currentTimeMillis();
            heartbeatMap.forEach((member, lastResponse) -> {
                if (member.equals(localMember))
                    return;

                if (time - lastResponse > 1000 * 10) {
                    clusterMembers.remove(member);
                    LOGGER.info("Member removed {}", member);
                    heartbeatMap.remove(member);
                    membershipListeners.forEach(l -> l.memberRemoved(member));
                }
            });
            multicastServer.writeAndFlush(new DatagramPacket(heatbeatBuf, multicastAddress, localMember.address));
        }, 500, 500, TimeUnit.MILLISECONDS);

        services = IntStream.range(0, serviceGenerators.size())
                .mapToObj(idx -> serviceGenerators.get(idx).constructor.newInstance(new ServiceContext((short) idx)))
                .collect(Collectors.toList());

        services.forEach(Service::onStart);
    }

    private void startServer(InetSocketAddress addr) throws InterruptedException {
        ChannelFuture bind = new ServerBootstrap()
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .childHandler(new SocketChannelChannelInitializer(this))
                .bind(addr);

        server = bind.sync()
                .addListener(future -> {
                    if (future.isSuccess()) {
                        LOGGER.info("Net server listening on {}", bind.channel().localAddress());
                    } else {
                        LOGGER.error("Failed to bind {}", bind.channel().localAddress());
                    }
                }).channel();
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

    public Collection<Member> getClusterMembers() {
        return Collections.unmodifiableCollection(clusterMembers);
    }

    public <T extends Service> T getService(Class<T> serviceClass) {
        return (T) services.stream().filter(service -> service.getClass().equals(serviceClass)).findAny().get();
    }


    public Member getLocalMember() {
        return localMember;
    }

    private CompletionStage<Result> send(String serverId, Object bytes, short service) {
        return sendInternal(getConnection(serverId), bytes, service);
    }

    private CompletionStage<Result> send(String serverId, Request request, short service) {
        return sendInternal(getConnection(serverId), request, service);
    }

    Member findMember(String nodeId) {
        return clusterMembers.stream().filter(n -> n.getId().equals(nodeId)).findAny().get();
    }

    private Map<Member, CompletionStage<Result>> sendAllMembersInternal(Object bytes, short service) {
        Map<Member, CompletionStage<Result>> map = new ConcurrentHashMap<>();
        clusterConnection.forEach((key, conn) -> {
            if(!key.equals(localMember.getId())) {
                LOGGER.debug("member {} ", findMember(key));
                map.put(findMember(key), sendInternal(conn, bytes, service));
            }
        });
        return map;
    }

    boolean memberExists(String nodeId) {
        return clusterMembers.stream().anyMatch(n -> n.getId().equals(nodeId));
    }

    public void close() throws InterruptedException {
        for (Channel entry : clusterConnection.values()) {
            entry.close().sync();
        }
        multicastServer.leaveGroup(multicastAddress, NetUtil.LOOPBACK_IF).sync();
        multicastServer.close().sync();
        server.close().sync();
        services.forEach(s -> s.onClose());
    }

    private CompletionStage<Result> sendInternal(Channel channel, Object obj, short service) {
        CompletableFuture<Result> future = new CompletableFuture<>();

        int andIncrement = messageSequence.getAndIncrement();
        Packet message = new Packet(andIncrement, obj, service);
        messageHandlers.put(andIncrement, future);

        channel.writeAndFlush(message);
        return future;
    }

    private Channel getConnection(String nodeId) {
        Channel channel = clusterConnection.get(nodeId);
        if (channel == null) {
            Optional<Member> node = clusterMembers.stream().filter(n -> n.getId().equals(nodeId)).findAny();
            node.orElseThrow(() -> new RuntimeException());

            Member member1 = node.get();
            Channel created = null;
            try {
                created = connectServer(member1.address);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            clusterConnection.put(member1.getId(), created);
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

    public void handleOperation(Operation o1) {

    }

    public static class HeartbeatOperation extends InternalRequest{
        public HeartbeatOperation(Member me) {
            sender = me;
        }

        @Override
        public void run(Cluster cluster, OperationContext ctx) {
            cluster.heartbeatMap.put(sender, System.currentTimeMillis());
        }
    }


    public static class AddMemberRequest extends InternalRequest {
        Member member;

        public AddMemberRequest(Member member, Member sender) {
            this.member = member;
            this.sender = sender;
        }

        public void run(Cluster cluster, OperationContext ctx) {
            if (!cluster.memberExists(member.getId())) {
                Channel channel;
                try {
                    channel = cluster.connectServer(member.getAddress());
                } catch (InterruptedException e) {
                    return;
                }
                cluster.clusterConnection.put(member.getId(), channel);
                cluster.clusterMembers.add(member);
                cluster.heartbeatMap.put(member, System.currentTimeMillis());
                cluster.membershipListeners.forEach(x -> x.memberAdded(member));
                LOGGER.info("welcome new member {}", member);
            }

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

    @Override
    public void onStart() {

    }

    @Override
    public void onClose() {

    }

    public class ServiceContext<T extends Service> {
        short service;

        public ServiceContext(short service) {
            this.service = service;
        }

        public CompletionStage<Result> send(String serverId, Object bytes) {
            return sendInternal(getConnection(serverId), bytes, service);
        }

        public CompletionStage<Result> send(String serverId, Request<T> request) {
            return sendInternal(getConnection(serverId), request, service);
        }

        public Map<Member, CompletionStage<Result>> sendAllMembers(Object bytes) {
            return sendAllMembersInternal(bytes, service);
        }

        public Map<Member, CompletionStage<Result>> sendAllMembers(Request<T> bytes) {
            return sendAllMembersInternal(bytes, service);
        }

        public Cluster getCluster() {
            return Cluster.this;
        }
    }
}
