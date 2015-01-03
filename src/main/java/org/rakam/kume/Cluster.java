package org.rakam.kume;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.EventExecutor;
import org.rakam.kume.service.Service;
import org.rakam.kume.service.ServiceConstructor;
import org.rakam.kume.transport.Packet;
import org.rakam.kume.transport.PacketDecoder;
import org.rakam.kume.transport.PacketEncoder;
import org.rakam.kume.util.Throwables;
import org.rakam.kume.util.Tuple;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.rakam.kume.Cluster.MemberState.MASTER;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/11/14 21:41.
 */
public class Cluster {
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    // IO thread for TCP and UDP connections
    final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    // Processor thread pool that deserialize/serialize incoming packets
    final EventLoopGroup workerGroup = new NioEventLoopGroup();
    // Thread pool for handling requests and messages
    final NioEventLoopGroupArray eventExecutors = new NioEventLoopGroupArray();
    // Event loop for running cluster migrations
    final EventExecutor eventLoop = new NioEventLoopGroup(1).next();

    final private List<Service> services;

    final private AtomicInteger messageSequence = new AtomicInteger();
    final protected ServiceContext<InternalService> internalBus;

    final ConcurrentHashMap<Member, Channel> clusterConnection = new ConcurrentHashMap<>();

    final Cache<Integer, CompletableFuture<Object>> messageHandlers = CacheBuilder.newBuilder()
            .expireAfterWrite(105, TimeUnit.SECONDS)
            .removalListener((RemovalNotification<Integer, CompletableFuture<Object>> notification) -> {
                if (!notification.getCause().equals(RemovalCause.EXPLICIT))
                    notification.getValue().completeExceptionally(new TimeoutException());
            }).build();
    final private Member localMember;
    final private MulticastServerHandler multicastServer;
    private final Map<String, Service> serviceNameMap;
    private Member master;
    private Set<Member> members;
    private long lastContactedTimeMaster;
    final private Channel server;
    private int lastVotedCursor;
    final private List<MembershipListener> membershipListeners = Collections.synchronizedList(new ArrayList<>());

    final private Map<Member, Long> heartbeatMap = new ConcurrentHashMap<>();
    final private long clusterStartTime;
    private ScheduledFuture<?> heartbeatTask;
    private ConcurrentMap<InetSocketAddress, Integer> pendingUserVotes = CacheBuilder.newBuilder().expireAfterWrite(100, TimeUnit.SECONDS).<InetSocketAddress, Integer>build().asMap();
    private MemberState memberState;

    public Cluster(Collection<Member> members, ServiceInitializer serviceGenerators, InetSocketAddress serverAddress, boolean mustJoinCluster) {
        clusterStartTime = System.currentTimeMillis();
        this.members = new HashSet<>(members);

        ChannelFuture bind = new ServerBootstrap()
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .option(ChannelOption.SO_BACKLOG, 100)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
                        p.addLast("packetDecoder", new PacketDecoder());
                        p.addLast("frameEncoder", new LengthFieldPrepender(4));
                        p.addLast("packetEncoder", new PacketEncoder());
                        p.addLast(new ServerChannelAdapter(Cluster.this));
                    }
                }).bind(serverAddress);

        try {
            server = bind.sync()
                    .addListener(future -> {
                        if (!future.isSuccess()) {
                            LOGGER.error("Failed to bind {}", bind.channel().localAddress());
                        }
                    }).channel();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to bind TCP "+bind.channel().localAddress());
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    server.closeFuture().sync();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        localMember = new Member((InetSocketAddress) server.localAddress());
        master = localMember;

        InetSocketAddress multicastAddress = new InetSocketAddress("224.0.67.67", 5001);
        try {
            multicastServer = new MulticastServerHandler(this, multicastAddress)
                    .start();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to bind UDP "+bind.channel().localAddress());
        }

        LOGGER.info("{} started , listening UDP multicast server {}", localMember, multicastAddress);

        if (mustJoinCluster) {
            joinCluster();
        }

        services = new ArrayList<>(serviceGenerators.size() + 16);
        InternalService internalService = new InternalService(new ServiceContext<>(0), this);
        services.add(internalService);
        internalBus = internalService.getContext();
        IntStream.range(0, serviceGenerators.size())
                .mapToObj(idx -> {
                    ServiceInitializer.Constructor c = serviceGenerators.get(idx);
                    ServiceContext bus = new ServiceContext(idx + 1, c.name);
                    return c.constructor.newInstance(bus);
                }).collect(Collectors.toCollection(() -> services));

        serviceNameMap = IntStream.range(0, serviceGenerators.size())
                .mapToObj(idx -> new Tuple<>(serviceGenerators.get(idx).name, services.get(idx + 1)))
                .collect(Collectors.toConcurrentMap(x -> x._1, x -> x._2));

        scheduleClusteringTask();
        server.config().setAutoRead(true);
        multicastServer.setAutoRead(true);
    }

    private void joinCluster() {
        multicastServer.sendMulticast(new JoinOperation());

        CompletableFuture<Boolean> latch = new CompletableFuture<>();
        AtomicInteger count = new AtomicInteger();
        workerGroup.scheduleAtFixedRate(() -> {
            multicastServer.sendMulticast(new JoinOperation());
            if (!master.equals(localMember)) {
                latch.complete(true);
                // this is a trick that stops this task. the exception will be swallowed.
                throw new RuntimeException("found cluster");
            }
            if (count.incrementAndGet() >= 20)
                latch.complete(false);
        }, 0, 250, TimeUnit.MILLISECONDS);

        memberState = memberState.FOLLOWER;
        if (!latch.join()) {
            throw new IllegalStateException("Could not found a cluster. You may disable mustJoinCluster.set(false) for creating new cluster.");
        }

    }

    public synchronized void addMemberInternal(Member member) {
        if (!members.contains(member) && !member.equals(localMember)) {
            LOGGER.info("Discovered new member {}", member);

            // we may create the connection before executing this method.
            if (!clusterConnection.containsKey(member)) {
                Channel channel;
                try {
                    channel = connectServer(member.getAddress());
                } catch (InterruptedException e) {
                    LOGGER.error("Couldn't connect new server", e);
                    return;
                }
                clusterConnection.put(member, channel);
            }

            members.add(member);
            if (isMaster())
                heartbeatMap.put(member, System.currentTimeMillis());
            membershipListeners.forEach(x ->  eventLoop.execute(Throwables.propagate(() -> x.memberAdded(member))));
        }
    }

    public synchronized void addMembersInternal(Set<Member> newMembers) {
        if (!members.containsAll(newMembers)) {
            LOGGER.info("Discovered another cluster of {} members", members.size());

            for (Member member : newMembers) {
                // we may create the connection before executing this method.
                if (!clusterConnection.containsKey(member)) {
                    Channel channel;
                    try {
                        channel = connectServer(member.getAddress());
                    } catch (InterruptedException e) {
                        LOGGER.error("Couldn't connect new server", e);
                        return;
                    }
                    clusterConnection.put(member, channel);
                }

                members.add(member);
                if (isMaster())
                    heartbeatMap.put(member, System.currentTimeMillis());
            }
            membershipListeners.forEach(x -> eventLoop.execute(Throwables.propagate(() -> x.clusterMerged(newMembers))));
        }
    }

    private void scheduleClusteringTask() {
        workerGroup.schedule(() -> {

        }, 10, TimeUnit.MILLISECONDS);

        heartbeatTask = workerGroup.scheduleAtFixedRate(() -> {
            long time = System.currentTimeMillis();

            if (isMaster()) {
                heartbeatMap.forEach((member, lastResponse) -> {
                    if (time - lastResponse > 2000) {
                        removeMemberAsMaster(member, true);
                    }
                });
//                members.forEach(member -> internalBus.send(member, new HeartbeatRequest()));
                multicastServer.sendMulticast(new HeartbeatRequest());
            } else {
                if (time - lastContactedTimeMaster > 500) {
                    workerGroup.schedule(() -> {
                        if (time - lastContactedTimeMaster > 500) {
                            memberState = MemberState.CANDIDATE;
                            voteElection();
                        }
                    }, 150 + new Random().nextInt(150), TimeUnit.MILLISECONDS);
                } else {
                    Member localMember = getLocalMember();
                    internalBus.send(master, (masterCluster, ctx) ->
                            masterCluster.cluster.heartbeatMap.put(localMember, System.currentTimeMillis()));
                }
            }

        }, 200, 200, TimeUnit.MILLISECONDS);

        workerGroup.scheduleWithFixedDelay(() -> {
            ClusterCheckAndMergeOperation req = new ClusterCheckAndMergeOperation(getMembers().size(), System.currentTimeMillis() - clusterStartTime);
            multicastServer.sendMulticast(req);
        }, 0, 2000, TimeUnit.MILLISECONDS);
    }

    public long startTime() {
        return clusterStartTime;
    }

    public void voteElection() {
        Collection<Member> clusterMembers = getMembers();

        Map<Member, Boolean> map = new ConcurrentHashMap<>();
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        int cursor = lastVotedCursor++;
        Map<Member, CompletableFuture<Boolean>> m = internalBus.askAllMembers((service, ctx) -> {
            ctx.reply(service.cluster.lastVotedCursor++ == cursor - 1);
        });

        m.forEach((member, resultFuture) -> {
            resultFuture.thenAccept(result -> {
                map.put(member, result);

                Map<Boolean, Long> stream = map.entrySet().stream()
                        .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.counting()));
                if (stream.getOrDefault(Boolean.TRUE, 0L) > clusterMembers.size() / 2) {
                    future.complete(true);
                } else if (stream.getOrDefault(Boolean.FALSE, 0L) > clusterMembers.size() / 2) {
                    future.complete(false);
                }

            });
        });

        if (future.join()) {
            memberState = MASTER;
            Member localMember = this.localMember;
            internalBus.sendAllMembers((service, ctx) -> service.cluster.changeMaster(localMember));
        }
    }

    public MemberState memberState() {
        return memberState;
    }

    private synchronized void changeMaster(Member masterMember) {
        master = masterMember;
        memberState = masterMember.equals(localMember) ? MASTER : MemberState.FOLLOWER;
        multicastServer.setJoinGroup(memberState == MASTER);
    }

    public void removeMemberAsMaster(Member member, boolean replicate) {
        if (!isMaster())
            throw new IllegalStateException();

        return;

//        heartbeatMap.remove(member);
//        members.remove(member);
//        if(replicate) {

//        internalBus.sendAllMembers((cluster, ctx) -> {
//            cluster.clusterConnection.remove(member);
//            Cluster.LOGGER.info("Member removed {}", member);
//            cluster.membershipListeners.forEach(l -> Throwables.propagate(() -> l.memberRemoved(member)));
//        }, true);
//        }
    }

    protected Channel connectServer(SocketAddress serverAddr) throws InterruptedException {
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
        // replace with collections and present a view instead of creating new HashSet
        Set<Member> members = new HashSet<>(this.members);
        members.add(localMember);
        return Collections.unmodifiableSet(members);
    }

    public <T extends Service> T getService(String serviceName) {
        checkNotNull(serviceName, "null is not allowed for service name");
        return (T) serviceNameMap.get(serviceName);
    }

    public <T extends Service> T getService(String serviceName, Class<T> clazz) {
        return getService(serviceName);
    }

    public <T extends Service> T createOrGetService(String name, ServiceConstructor<T> ser) {
        checkNotNull(ser, "null is not allowed for service constructor");
        Service existingService = serviceNameMap.get(name);
        if(existingService != null)
            return (T) existingService;
        int maxSize = Short.MAX_VALUE * 2;
        checkState(services.size() < maxSize, "Maximum number of allowed services is %s", maxSize);

        int id = services.size();

        Request<InternalService, Boolean> createServiceRequest = (service, ctx) -> {
            T s = ser.newInstance(new ServiceContext(services.size(), name));
            // service is not thread-safe
            synchronized (service.cluster.services) {
                service.cluster.services.add(s);
            }
            if(name!=null)
                service.cluster.serviceNameMap.put(name, s);
            ctx.reply(true);
        };

        for (Member member : getMembers()) {
            internalBus.tryAskUntilDone(member, createServiceRequest, 5)
                    .whenComplete((result, ex) -> {
                        if (ex != null && ex instanceof TimeoutException) {
                            removeMemberAsMaster(member, true);
                        }
                    });
        }

        return (T) services.get(id);
    }

    public <T extends Service> T createService(ServiceConstructor<T> ser) {
        return createOrGetService(null, ser);
    }

    public boolean destroyService(String serviceName) {
        checkNotNull(serviceName, "null is not allowed for service name");
        Service service = serviceNameMap.remove(serviceName);
        if (service == null)
            return false;

        service.onClose();
        int serviceId = services.indexOf(service);
        // we do not shift the array because if the indexes change, we have to ensure consensus among nodes.
        services.set(serviceId, null);
        return true;
    }

    public Member getLocalMember() {
        return localMember;
    }

    private void send(Member server, Object bytes, int service) {
        sendInternal(getConnection(server), bytes, service);
    }

    public void sendAllMembersInternal(Object bytes, int service) {
        clusterConnection.forEach((member, conn) -> {
            if (!member.equals(localMember)) {
                sendInternal(conn, bytes, service);
            }
        });
    }

    public <R> Map<Member, CompletableFuture<R>> askAllMembersInternal(Object bytes, int service) {
        Map<Member, CompletableFuture<R>> map = new ConcurrentHashMap<>();
        clusterConnection.forEach((member, conn) -> {
            if (!member.equals(localMember)) {
                map.put(member, askInternal(conn, bytes, service));
            }
        });
        return map;
    }

    public void close() throws InterruptedException {
        for (Channel entry : clusterConnection.values()) {
            entry.close().sync();
        }
        multicastServer.close();
        server.close().sync();
        heartbeatTask.cancel(true);
        services.forEach(s -> s.onClose());
        workerGroup.shutdownGracefully().await();
    }

    public void sendInternal(Channel channel, Object obj, int service) {
        Packet message = new Packet(obj, service);
        channel.writeAndFlush(message);
    }

    public <R> CompletableFuture<R> askInternal(Channel channel, Object obj, int service) {
        CompletableFuture future = new CompletableFuture<>();

        int andIncrement = messageSequence.getAndIncrement();
        Packet message = new Packet(andIncrement, obj, service);
        messageHandlers.put(andIncrement, future);

        channel.writeAndFlush(message);
        return future;
    }

    private Channel getConnection(Member member) {
        Channel channel = clusterConnection.get(member);
        if (channel == null) {
            if (!members.contains(member))
                throw new IllegalArgumentException("the member doesn't exist in the cluster");

            Channel created;
            try {
                created = connectServer(member.address);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
            synchronized (this) {
                clusterConnection.put(member, created);
            }
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

    public static class HeartbeatRequest implements Operation<InternalService> {

        @Override
        public void run(InternalService service, OperationContext ctx) {
            Member sender = ctx.getSender();
            Member masterMember = service.cluster.getMaster();
            if(sender == null) {
                return;
            }
            if (sender.equals(masterMember)) {
                service.cluster.lastContactedTimeMaster = System.currentTimeMillis();
            } else {
                if (!service.cluster.getMembers().contains(sender)) {
                    service.cluster.changeMaster(ctx.getSender());
                }
            }
        }
    }

    public void pause() {
        server.config().setAutoRead(false);
    }

    public void resume() {
        server.config().setAutoRead(true);
    }

    public class InternalService extends Service {
        protected final Cluster cluster;
        private final ServiceContext<InternalService> ctx;

        private InternalService(ServiceContext<InternalService> ctx, Cluster cluster) {
            this.ctx = ctx;
            this.cluster = cluster;
        }

        public ServiceContext<InternalService> getContext() {
            return ctx;
        }

        @Override
        public void onClose() {

        }
    }

    public class ServiceContext<T extends Service> {
        private final String serviceName;
        private final int service;

        public ServiceContext(int service, String serviceName) {
            this.service = service;
            this.serviceName = serviceName;
        }
        public ServiceContext(int service) {
            this.service = service;
            this.serviceName = null;
        }

        public void send(Member server, Object bytes) {
            if (server.equals(localMember)) {
                LocalOperationContext ctx1 = new LocalOperationContext(null, service, localMember);
                // move to an executor handler
                services.get(service).handle(ctx1, bytes);
            } else {
                sendInternal(getConnection(server), bytes, service);
            }
        }

        public <R> void send(Member server, Request<T, R> request) {
            if (server.equals(localMember)) {
                CompletableFuture<R> f = new CompletableFuture<>();
                Service s = services.get(service);
                LocalOperationContext ctx = new LocalOperationContext(f, service, localMember);
                workerGroup.execute(() -> s.handle(ctx, request));
            } else {
                sendInternal(getConnection(server), request, service);
            }
        }

        public void sendAllMembers(Object bytes) {
            sendAllMembers(bytes, false);
        }

        public <R> void sendAllMembers(Request<T, R> bytes) {
            sendAllMembers(bytes, false);
        }

        public <R> void sendAllMembers(Object bytes, boolean includeThisMember) {
            sendAllMembersInternal(bytes, service);

            if (includeThisMember) {
                CompletableFuture<R> f = new CompletableFuture<>();
                Service s = services.get(service);
                LocalOperationContext ctx = new LocalOperationContext(f, service, localMember);
                workerGroup.execute(() -> s.handle(ctx, bytes));
            }
        }

        public <R> void sendAllMembers(Request<T, R> bytes, boolean includeThisMember) {
            sendAllMembersInternal(bytes, service);

            if (includeThisMember) {
                CompletableFuture<R> f = new CompletableFuture<>();
                Service s = services.get(service);
                LocalOperationContext ctx = new LocalOperationContext(f, service, localMember);
                workerGroup.execute(() -> s.handle(ctx, bytes));
            }
        }

        public int serviceId() {
            return service;
        }

        public String serviceName() {
            return serviceName;
        }

        public <R> CompletableFuture<R> ask(Member server, Object bytes) {
            if (server.equals(localMember)) {
                CompletableFuture<R> future = new CompletableFuture<>();
                LocalOperationContext ctx1 = new LocalOperationContext(future, service, localMember);
                // move to an executor handler
                services.get(service).handle(ctx1, bytes);
                return future;
            } else {
                return askInternal(getConnection(server), bytes, service);
            }
        }

        public <R> CompletableFuture<R> ask(Member server, Request<T, R> request) {
            if (server.equals(localMember)) {
                CompletableFuture<R> future = new CompletableFuture<>();
                LocalOperationContext ctx1 = new LocalOperationContext(future, service, localMember);
                request.run((T) services.get(service), ctx1);
                return future;
            } else {
                return askInternal(getConnection(server), request, service);
            }
        }

        public <R> CompletableFuture<R> ask(Member server, Request<T, R> request, Class<R> clazz) {
            return ask(server, request);
        }

        public <R> Map<Member, CompletableFuture<R>> askAllMembers(Object bytes) {
            return askAllMembersInternal(bytes, service);
        }

        public <R> Map<Member, CompletableFuture<R>> askAllMembers(Request<T, R> bytes, boolean includeThisMember) {
            Map<Member, CompletableFuture<R>> m = askAllMembersInternal(bytes, service);

            if (includeThisMember) {
                CompletableFuture<R> f = new CompletableFuture<>();
                Service s = services.get(service);
                LocalOperationContext ctx = new LocalOperationContext(f, service, localMember);
                workerGroup.execute(() -> s.handle(ctx, bytes));
                m.put(localMember, f);
            }

            return m;
        }

        public <R> Map<Member, CompletableFuture<R>> askAllMembers(Object bytes, boolean includeThisMember) {
            Map<Member, CompletableFuture<R>> m = askAllMembersInternal(bytes, service);

            if (includeThisMember) {
                CompletableFuture<R> f = new CompletableFuture<>();
                Service s = services.get(service);
                LocalOperationContext ctx = new LocalOperationContext(f, service, localMember);
                workerGroup.execute(() -> s.handle(ctx, bytes));
                m.put(localMember, f);
            }
            return m;
        }

        public <R> Map<Member, CompletableFuture<R>> askAllMembers(Request<T, R> bytes) {
            return askAllMembers(bytes, true);
        }

        public Cluster getCluster() {
            return Cluster.this;
        }

        private <R> void tryAskUntilDone(Member member, Request<T, R> req, int numberOfTimes, CompletableFuture<R> future) {
            ask(member, req).whenComplete((val, ex) -> {
                if (ex != null)
                    if (ex instanceof TimeoutException) {
                        if (numberOfTimes == 0) {
                            future.completeExceptionally(new TimeoutException());
                        } else {
                            tryAskUntilDone(member, req, numberOfTimes, future);
                        }
                    } else {
                        future.completeExceptionally(ex);
                    }
                else
                    future.complete(val);
            });
        }

        public <R> CompletableFuture<R> tryAskUntilDone(Member member, Request<T, R> req, int numberOfTimes) {
            CompletableFuture<R> f = new CompletableFuture<>();
            tryAskUntilDone(member, req, numberOfTimes, f);
            return f;
        }

        public <R> CompletableFuture<R> tryAskUntilDone(Member member, Request<T, R> req, int numberOfTimes, Class<R> clazz) {
            return tryAskUntilDone(member, req, numberOfTimes);
        }

    }

    public static class JoinOperation implements Operation<InternalService> {

        @Override
        public void run(InternalService service, OperationContext<Void> ctx) {
            synchronized (service.cluster) {
                Channel channel;
                Member newMember = ctx.getSender();
                try {
                    channel = service.cluster.connectServer(newMember.address);
                } catch (InterruptedException e) {
                    return;
                }
                service.cluster.clusterConnection.put(newMember, channel);
                service.cluster.addMemberInternal(newMember);

                Member masterMember = service.cluster.getMaster();
                Set<Member> members = service.cluster.getMembers();
                Request<InternalService, Object> changeClusterOfNewMember = (service0, ctx0) ->
                        service0.cluster.changeCluster(members, masterMember, true);
                Request<InternalService, Object> addNewMember = (service0, ctx0) ->
                        service0.cluster.addMemberInternal(newMember);

                for (Member member : service.cluster.getMembers()) {
                    service.cluster.internalBus.tryAskUntilDone(member, member.equals(newMember) ? changeClusterOfNewMember : addNewMember, 5)
                            .whenComplete((result, ex) -> {
                                if (ex != null && ex instanceof TimeoutException) {
                                    service.cluster.removeMemberAsMaster(member, true);
                                }
                            });
                }
            }
        }
    }

    protected synchronized void changeCluster(Set<Member> newClusterMembers, Member masterMember, boolean isNew) {
        try {
            pause();
            clusterConnection.clear();
            master = masterMember;
            members = newClusterMembers;
            messageHandlers.cleanUp();
            LOGGER.info("Joined a cluster of {} nodes.", members.size());
            multicastServer.setJoinGroup(masterMember.equals(localMember));
            if(!isNew)
                membershipListeners.forEach(x -> eventLoop.execute(Throwables.propagate(() -> x.clusterChanged())));
        } finally {
            resume();
        }
    }

    public static enum MemberState {
        FOLLOWER, CANDIDATE, MASTER
    }
}
