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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/11/14 21:41.
 */
public class Cluster implements Service {
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    final EventLoopGroup workerGroup = new NioEventLoopGroup();

    final private List<Service> services;

    final private AtomicInteger messageSequence = new AtomicInteger();
    final private ServiceContext<Cluster> internalBus = new ServiceContext<>(0, "internal");

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

    public Cluster(Collection<Member> members, ServiceInitializer serviceGenerators, InetSocketAddress serverAddress, boolean mustJoinCluster) throws InterruptedException {
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
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        localMember = new Member((InetSocketAddress) server.localAddress());
        master = localMember;

        InetSocketAddress multicastAddress = new InetSocketAddress("224.0.67.67", 5001);
        multicastServer = new MulticastServerHandler(this, multicastAddress)
                .start();

        LOGGER.info("{} started , listening UDP multicast server {}", localMember, multicastAddress);

        if (mustJoinCluster) {
            joinCluster();
        }

        services = new ArrayList<>(serviceGenerators.size() + 16);
        services.add(this);
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

    public void joinCluster() {
        multicastServer.sendMulticast(new JoinOperation());

        CompletableFuture<Boolean> latch = new CompletableFuture<>();
        AtomicInteger count = new AtomicInteger();
        workerGroup.scheduleAtFixedRate(() -> {
            multicastServer.sendMulticast(new JoinOperation());
            if (!master.equals(localMember)) {
                // this is a trick that stops this task. the exception will be swallowed.
                latch.complete(true);
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
            membershipListeners.forEach(x -> Throwables.propagate(() -> x.memberAdded(member)));
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
            membershipListeners.forEach(x -> Throwables.propagate(() -> x.clusterMerged(newMembers)));
        }
    }

    private void scheduleClusteringTask() throws InterruptedException {
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
                            masterCluster.heartbeatMap.put(localMember, System.currentTimeMillis()));
                }
            }

        }, 200, 200, TimeUnit.MILLISECONDS);

        workerGroup.scheduleWithFixedDelay(() -> {
            ClusterCheckAndMergeOperation req = new ClusterCheckAndMergeOperation(getMembers().size(), System.currentTimeMillis() - clusterStartTime);
            multicastServer.sendMulticast(req);
        }, 0, 2000, TimeUnit.MILLISECONDS);
    }

    public void voteElection() {
        Collection<Member> clusterMembers = getMembers();

        Map<Member, Boolean> map = new ConcurrentHashMap<>();
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        int cursor = lastVotedCursor++;
        Map<Member, CompletableFuture<Boolean>> m = internalBus.askAllMembers((cluster, ctx) -> {
            ctx.reply(cluster.lastVotedCursor++ == cursor - 1);
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
            memberState = MemberState.MASTER;
            Member localMember = this.localMember;
            internalBus.sendAllMembers((cluster, ctx) -> cluster.changeMaster(localMember));
        }
    }

    public MemberState memberState() {
        return memberState;
    }

    private synchronized void changeMaster(Member masterMember) {
        master = masterMember;
        memberState = masterMember.equals(localMember) ? MemberState.MASTER : MemberState.FOLLOWER;

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
        // replace with collections and present a view instead of creating new HashSet
        Set<Member> members = new HashSet<>(this.members);
        members.add(localMember);
        return Collections.unmodifiableSet(members);
    }

    public <T extends Service> T getService(String serviceName) {
        return (T) serviceNameMap.get(serviceName);
    }

    public <T extends Service> T createService(String name, ServiceConstructor<T> ser) throws InterruptedException {
        int size = services.size();
        if (size == Short.MAX_VALUE) {
            throw new IllegalStateException("Maximum number of allowed services is " + Short.MAX_VALUE);
        }

        Request<Cluster, Boolean> bytes = (service, ctx) -> {
            T s = ser.newInstance(new ServiceContext(size, name));
            service.services.add(s);
            service.serviceNameMap.put(name, s);
            ctx.reply(true);
        };
        Map<Member, CompletableFuture<Boolean>> m = internalBus.askAllMembers(bytes, true);

        CountDownLatch latch = new CountDownLatch(m.size());

        m.forEach((key, value) -> tryUntilDone(latch, key, value, bytes));

        latch.await();
        return (T) serviceNameMap.get(name);
    }

    private void tryUntilDone(CountDownLatch latch, Member member, CompletableFuture<Boolean> future, Request req) {
        future.whenComplete((x, ex) -> {
            if (ex == null) {
                tryUntilDone(latch, member, internalBus.ask(member, req), req);
            } else {
                latch.countDown();
            }
        });
    }

    public boolean destroyService(String serviceName) {
        Service service = serviceNameMap.remove(serviceName);
        if (service == null)
            return false;

        service.destroy();
        int serviceId = services.indexOf(service);
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

    public static class HeartbeatRequest implements Operation<Cluster> {

        @Override
        public void run(Cluster cluster, OperationContext ctx) {
            Member sender = ctx.getSender();
            Member masterMember = cluster.getMaster();
            if (sender.equals(masterMember)) {
                cluster.lastContactedTimeMaster = System.currentTimeMillis();
            } else {
                if (!cluster.getMembers().contains(sender)) {
//                    CompletableFuture<HashMap> ask = cluster.internalBus.ask(sender, (service, ctx0) -> {
//                        long runningTime = service.clusterStartTime;
//                        HashMap map = new HashMap();
//                        map.put("members", service.getMembers());
//                        map.put("runningTime", runningTime);
//                        map.put("master", service.master);
//                        ctx0.reply(map);
//                    });
//
//                    ask.thenAccept(data -> {
//                        int myClusterSize = cluster.getMembers().size() - 1;
//                        Set<Member> otherMembers = (Set<Member>) data.get("members");
//                        int otherClusterSize = otherMembers.size();
//                        if (otherMembers.contains(cluster.getLocalMember()))
//                            otherClusterSize--;
//
//                        boolean canJoin = otherClusterSize > myClusterSize;
//
//                        if (otherClusterSize == myClusterSize) {
//                            long myRunningTime = System.currentTimeMillis() - cluster.clusterStartTime;
//                            canJoin |= (Long) data.get("runningTime") < myRunningTime;
//                        }
//
//                        if (canJoin) {
//                            cluster.master = (Member) data.get("master");
//                        }
//
//                        otherMembers.forEach(cluster::addMemberInternal);
//
//                    });


                } else {

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

    public class ServiceContext<T extends Service> {
        private final String serviceName;
        private final int service;

        public ServiceContext(int service, String serviceName) {
            this.service = service;
            this.serviceName = serviceName;

        }

        public void send(Member server, Object bytes) {
            if (server.equals(localMember)) {
                LocalOperationContext ctx1 = new LocalOperationContext(null, localMember);
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
                LocalOperationContext ctx = new LocalOperationContext(f, localMember);
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
                LocalOperationContext ctx = new LocalOperationContext(f, localMember);
                workerGroup.execute(() -> s.handle(ctx, bytes));
            }
        }

        public <R> void sendAllMembers(Request<T, R> bytes, boolean includeThisMember) {
            sendAllMembersInternal(bytes, service);

            if (includeThisMember) {
                CompletableFuture<R> f = new CompletableFuture<>();
                Service s = services.get(service);
                LocalOperationContext ctx = new LocalOperationContext(f, localMember);
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
                LocalOperationContext ctx1 = new LocalOperationContext(future, localMember);
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
                LocalOperationContext ctx1 = new LocalOperationContext(future, localMember);
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
                LocalOperationContext ctx = new LocalOperationContext(f, localMember);
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
                LocalOperationContext ctx = new LocalOperationContext(f, localMember);
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

        public long startTime() {
            return clusterStartTime;
        }

        private <R> void tryAskUntilDone(Member member, Request<?, R> req, int numberOfTimes, CompletableFuture<R> future) {
            CompletableFuture<R> ask = ask(member, req);
            ask.whenComplete((val, ex) -> {
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

    public static class JoinOperation implements Operation<Cluster> {

        @Override
        public void run(Cluster cluster, OperationContext<Void> ctx) {
            synchronized (cluster) {
                Channel channel;
                Member newMember = ctx.getSender();
                try {
                    channel = cluster.connectServer(newMember.address);
                } catch (InterruptedException e) {
                    return;
                }
                cluster.clusterConnection.put(newMember, channel);
                cluster.addMemberInternal(newMember);

                Member masterMember = cluster.getMaster();
                Set<Member> members = cluster.getMembers();
                Request<Cluster, Object> changeClusterOfNewMember = (service, ctx0) -> {
                    service.changeMaster(masterMember);
                    members.forEach(member -> service.addMemberInternal(member));
                };

                Request<Cluster, Object> addNewMember = (service, ctx0) -> service.addMemberInternal(newMember);

                for (Member member : cluster.getMembers()) {
                    cluster.internalBus.tryAskUntilDone(member, member.equals(newMember) ? changeClusterOfNewMember : addNewMember, 5)
                            .whenComplete((result, ex) -> {
                                if (ex != null && ex instanceof TimeoutException) {
                                    cluster.removeMemberAsMaster(member, true);
                                }
                            });
                }
            }
        }
    }


    public static class ClusterCheckAndMergeOperation implements Operation<Cluster> {

        private final int clusterSize;
        private final long timeRunning;

        public ClusterCheckAndMergeOperation(int clusterSize, long timeRunning) {
            this.clusterSize = clusterSize;
            this.timeRunning = timeRunning;
        }

        @Override
        public void run(Cluster cluster, OperationContext<Void> ctx) {
            Member sender = ctx.getSender();
            Set<Member> clusterMembers = cluster.getMembers();
            if (clusterMembers.contains(sender))
                return;

            if (clusterMembers.size() > clusterSize)
                return;

            long myTimeRunning = System.currentTimeMillis() - cluster.clusterStartTime;
            if (clusterMembers.size() == clusterSize && myTimeRunning < timeRunning)
                return;

            Channel channel;
            try {
                channel = cluster.connectServer(ctx.getSender().address);
            } catch (InterruptedException e) {
                return;
            }

            cluster.clusterConnection.put(ctx.getSender(), channel);
            cluster.addMemberInternal(ctx.getSender());

            final Set<Member> otherClusterMembers;
            try {
                otherClusterMembers = cluster.internalBus.tryAskUntilDone(ctx.getSender(), (service, ctx0) -> {
                    ctx0.reply(service.getMembers());
                }, 5, Set.class).join();
            } catch (CompletionException e) {
                cluster.removeMemberAsMaster(ctx.getSender(), false);
                return;
            }

            for (Member otherClusterMember : otherClusterMembers) {
                if (otherClusterMember.equals(ctx.getSender()))
                    continue;
                if(clusterMembers.contains(otherClusterMember)) {
                    otherClusterMembers.remove(otherClusterMember);
                    continue;
                }

                Channel memberChannel;
                try {
                    memberChannel = cluster.connectServer(ctx.getSender().address);
                } catch (InterruptedException e) {
                    otherClusterMembers.remove(ctx.getSender());
                    continue;
                }
                cluster.clusterConnection.put(ctx.getSender(), memberChannel);
                cluster.addMemberInternal(ctx.getSender());
            }

            cluster.getMembers().stream()
                    .map(member -> cluster.internalBus
                            .tryAskUntilDone(member, (service, ctx2) -> service.addMembersInternal(otherClusterMembers), 5)
                            .whenComplete((result, ex) -> {
                                if (ex != null && ex instanceof TimeoutException)
                                    cluster.removeMemberAsMaster(member, true);
                            })).forEach(CompletableFuture::join);
        }

    }

    private synchronized void changeCluster(Set<Member> newClusterMembers, Member masterMember) {
        try {
            pause();
            clusterConnection.clear();
            master = masterMember;
            members = newClusterMembers;
            messageHandlers.cleanUp();
            LOGGER.info("Joined a cluster of {} nodes.", members.size());
            membershipListeners.forEach(x -> Throwables.propagate(() -> x.clusterChanged()));
        } finally {
            resume();
        }
    }

    public static enum MemberState {
        FOLLOWER, CANDIDATE, MASTER
    }
}
