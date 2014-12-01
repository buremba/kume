package org.rakam.kume.service.ringmap;

import io.netty.channel.ChannelFuture;
import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.MembershipListener;
import org.rakam.kume.MigrationListener;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;
import org.rakam.kume.Result;
import org.rakam.kume.service.Service;
import org.rakam.kume.util.ConsistentHashRing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RingMap implements MembershipListener, Service {
    final static int SERVICE_ID = 0;
    final static Logger LOGGER = LoggerFactory.getLogger(RingMap.class);


    ConcurrentHashMap<String, Integer>[] map;
    // it's size will always be numberOfBucketsPerNode * replicationFactor
    int[] bucketIds;
    ConsistentHashRing ring;
    Cluster.ServiceContext<RingMap> cluster;

    private final Member localMember;
    private boolean paused;
    private final int replicationFactor;
    private LinkedList<MigrationListener> migrationListeners = new LinkedList<>();

    public RingMap(Cluster.ServiceContext cluster, int replicationFactor) {
        this.cluster = cluster;
        this.replicationFactor = replicationFactor;

        cluster.getCluster().addMembershipListener(this);
        localMember = cluster.getCluster().getLocalMember();

        map = Stream.generate(() -> new ConcurrentHashMap<>()).limit(8*replicationFactor)
                .toArray(value -> new ConcurrentHashMap[8*replicationFactor]);
        setRing(new ConsistentHashRing(cluster.getCluster().getMembers(), 8, replicationFactor));
    }

    private void setRing(ConsistentHashRing newRing) {
        ring = newRing;
        bucketIds = ring.getBuckets().entrySet().stream()
                .filter(entry -> entry.getValue().contains(localMember))
                .mapToInt(entry -> entry.getKey().id).toArray();
    }

    @Override
    public void memberAdded(Member member) {
//        cluster.getCluster().pause();

        try {
            Result clusterInformation = cluster.ask(member, new Request<RingMap>() {
                @Override
                public void run(RingMap service, OperationContext ctx) {
                    Map<String, Number> hashMap = new HashMap();
                    hashMap.put("memberCount", service.cluster.getCluster().getMembers().size());
                    hashMap.put("startTime", service.cluster.startTime());
                    ctx.reply(hashMap);
                }
            }).get();

            if (clusterInformation.isSucceeded()) {
                Map<String, Number> data = (Map<String, Number>) clusterInformation.getData();
                int myClusterSize = cluster.getCluster().getMembers().size() - 1;
                int otherClusterSize = data.get("memberCount").intValue();
                if (otherClusterSize > myClusterSize) {
                    joinCluster(member);
                    return;
                } else if (otherClusterSize == myClusterSize) {
                    long startTime = data.get("startTime").longValue();
                    if (startTime < cluster.startTime()) {
                        joinCluster(member);
                        return;
                    }
                }
                addMember(member);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    private void joinCluster(Member oneMemberOfCluster) {
        Result result = cluster.ask(oneMemberOfCluster, new Request<RingMap>() {
            @Override
            public void run(RingMap service, OperationContext ctx) {
                ctx.reply(service.getRing());
            }
        }).join();
        if (result.isSucceeded()) {
            ring = (ConsistentHashRing) result.getData();
            changeRing(ring.addNode(localMember));
        }
    }

    private CompletableFuture<Void> changeRing(ConsistentHashRing newRing) {
        ConsistentHashRing oldRing = ring;
        Map<ConsistentHashRing.TokenRange, List<Member>> oldBuckets = oldRing.getBuckets();

        ArrayList<CompletableFuture> migrations = new ArrayList<>();
        newRing.getBuckets().forEach((range, members) -> {
            if (members.contains(localMember)) {
                List<Member> oldMembers = oldBuckets.get(range);
                if (oldMembers == null) {
                    int start = oldRing.findClosest(range.start);
                    int end = oldRing.findClosest(range.end);
                    int index = members.indexOf(localMember);

                    for (int bucketId = start; bucketId < end; bucketId++) {
                        ArrayList<Member> oldBucketMembers = oldRing.getBucket(bucketId).members;
                        Member oldMember = oldBucketMembers.get(index % oldBucketMembers.size());

                        final int finalBucketId = bucketId;
                        CompletableFuture<Void> f = cluster.ask(oldMember, new Request<RingMap>() {
                            int bucketId = finalBucketId;

                            @Override
                            public void run(RingMap service, OperationContext ctx) {
                                Map<String, Integer> moveEntries = new HashMap<>();
                                Map<String, Integer> partition = service.getPartition(bucketId);
                                ConsistentHashRing serviceRing = service.ring;
                                Member actualMember = service.cluster.getCluster().getLocalMember();

                                partition.forEach((key, value) -> {
                                    ArrayList<Member> newMembers = serviceRing.findBucket(key).members;
                                    if (!newMembers.contains(localMember)) {
                                        partition.remove(key);
                                    }
                                    if(newMembers.contains(actualMember)) {
                                        moveEntries.put(key, value);
                                    }
                                });
                                if (moveEntries.size() > 0) {
                                    migrationListeners.forEach(l -> l.migrationStart(localMember));
                                    ctx.reply(moveEntries);
                                }
                            }
                        }).thenAccept(result -> {
                            if (result.isSucceeded()) {
                                Map<String, Integer> data = (Map) result.getData();
                                getPartition(finalBucketId).putAll(data);

                                LOGGER.info("Successfully completed migration. {} elements moved from {} to {}", data.size(), oldMember, localMember);
                                migrationListeners.forEach(l -> l.migrationEnd(localMember));

                            } else {
                                // TODO: try again until we the member removed from the cluster
                            }
                        });

                        migrations.add(f);

                    }

                } else if (!oldMembers.contains(localMember)) {
                    // we don't remove the old entries because
                    // the new member will request the entries and remove them via migration request
                }
            }

        });

        // combine all migrations
        return CompletableFuture.allOf(migrations.toArray(new CompletableFuture[migrations.size()]))
                .thenAccept(x -> {
                    LOGGER.debug("completed {} migrations. new ring has {} buckets in member {}",
                            migrations.size(), newRing.getBuckets().size(), localMember);
//                    setRing(newRing);
                });
    }

    private void addMember(Member member) {
        ConsistentHashRing newRing = ring.addNode(member);
        changeRing(newRing).
                thenAccept(x -> cluster.getCluster().resume());
    }

    Map<String, Integer> getPartition(int bucketId) {
        try {
            return map[Arrays.binarySearch(bucketIds, bucketId)];
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void memberRemoved(Member member) {
        changeRing(ring.removeNode(member)).thenAccept(x -> cluster.getCluster().resume());
    }

    @Override
    public void handle(OperationContext ctx, Object request) {
        //
    }

    @Override
    public void onStart() {
    }

    @Override
    public void onClose() {
        Arrays.stream(map).forEach(x -> x.clear());
        paused = true;
    }

    public List<CompletableFuture<Void>> putAll(Map<String, Integer> fromMap) {
        // this is not the way it should be, we should group map entries and send batches to members.
        List<CompletableFuture<Void>> objects =
                fromMap.entrySet().stream().map(entry -> put(entry.getKey(), entry.getValue()))
                        .collect(Collectors.<CompletableFuture<Void>>toList());
        return objects;
    }

    public CompletableFuture<Void> put(String key, Integer val) {
        int bucketId = ring.findBucketId(key);
        ConsistentHashRing.Bucket bucket = ring.getBucket(bucketId);

        CompletableFuture<Void> f = new CompletableFuture<>();

        ChannelFuture[] stages = new ChannelFuture[bucket.members.size()];
        int idx = 0;
        final int[] resultArr = {0};
        boolean onlyLocal = true;
        for (Member next : bucket.members) {
            if (next.equals(localMember)) {
                getPartition(bucketId).put(key, val);
            } else {
                stages[idx] = cluster.send(next, new PutMapOperation(key, val)).addListener(x -> {
                    resultArr[0]++;
                    // Since we use eventual consistency, we only wait quorum of servers to perform the operation.
                    if (resultArr[0] > stages.length / 2) {
                        f.complete(null);
                    }
                });
                onlyLocal = false;
            }
        }

        if (onlyLocal)
            f.complete(null);

        return f;
    }

    public CompletableFuture<Result> get(String key) {
        int bucketId = ring.findBucketId(key);
        ConsistentHashRing.Bucket bucket = ring.getBucket(bucketId);

        if (bucket.members.contains(localMember)) {
            return CompletableFuture.completedFuture(new Result(getPartition(bucketId).get(key)));
        }

        return cluster.ask(bucket.members.iterator().next(),
                (service, ctx) -> service.getPartition(ring.findBucketId(key)).get(key));
    }

    public Map<String, Integer>[] localPartitions() {
        Map<String, Integer>[] readOnly = new Map[map.length];

        for (int i = 0; i < map.length; i++)
            readOnly[i] = Collections.unmodifiableMap(map[i]);

        return readOnly;
    }

    public int getLocalSize() {
        return Arrays.stream(map).collect(Collectors.summingInt(value -> value.size()));
    }


    public CompletableFuture<Map<Member, Integer>> size() {
        Request<RingMap> bytes = new Request<RingMap>() {
            @Override
            public void run(RingMap service, OperationContext ctx) {
                System.out.println("remote " + service.getLocalSize());

                ctx.reply(service.getLocalSize());
            }
        };
        Map<Member, CompletableFuture<Result>> resultMap = cluster.askAllMembers(bytes);

        Map<Member, Integer> m = new ConcurrentHashMap<>(cluster.getCluster().getMembers().size());
        System.out.println("local " + getLocalSize());
        m.put(localMember, getLocalSize());
        CompletableFuture<Map<Member, Integer>> future = new CompletableFuture<>();

        resultMap.forEach((key, f) -> f.thenAccept(x -> {
            if (x.isSucceeded()) {
                m.put(key, (Integer) x.getData());
                resultMap.remove(key);
                if (resultMap.size() == 0) {
                    future.complete(m);
                }
            } else {
                //
            }
        }));

        return future;
    }

    public ConsistentHashRing getRing() {
        return ring;
    }

    public void listenMigrations(MigrationListener migrationListener) {
        migrationListeners.add(migrationListener);
    }


    public static class PutMapOperation implements Request<RingMap> {
        String key;
        Integer value;

        public PutMapOperation(String key, Integer value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void run(RingMap service, OperationContext ctx) {
            service.putLocal(key, value);
        }
    }

    private void putLocal(String key, Integer value) {
        getPartition(ring.findBucketId(key)).put(key, value);
    }

    public static class MigrationRequest implements Request<RingMap> {
        int bucketId;

        public MigrationRequest(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void run(RingMap service, OperationContext ctx) {
            ctx.reply(service.getPartition(bucketId));
        }

    }
}