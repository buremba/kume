package org.rakam.kume.service.ringmap;

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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
    private final int replicationFactor;
    private LinkedList<MigrationListener> migrationListeners = new LinkedList<>();
    private Map<ConsistentHashRing.TokenRange, Map<String, Integer>> dataWaitingForMigration = new HashMap<>();

    public RingMap(Cluster.ServiceContext cluster, int replicationFactor) {
        this.cluster = cluster;
        this.replicationFactor = replicationFactor;

        cluster.getCluster().addMembershipListener(this);
        localMember = cluster.getCluster().getLocalMember();

        map = createEmptyMap();

        ConsistentHashRing newRing = new ConsistentHashRing(cluster.getCluster().getMembers(), 8, replicationFactor);
        ring = newRing;
        bucketIds = createBucketForRing(newRing);
    }

    private int[] createBucketForRing(ConsistentHashRing ring) {
        return ring.getBuckets().entrySet().stream()
                .filter(entry -> entry.getValue().contains(localMember))
                .mapToInt(entry -> entry.getKey().id).sorted().toArray();
    }

    private ConcurrentHashMap[] createEmptyMap() {
        return Stream.generate(() -> new ConcurrentHashMap<>()).limit(8 * replicationFactor)
                .toArray(value -> new ConcurrentHashMap[8 * replicationFactor]);
    }

    private void printMapSize() {
        System.out.print("map: ");
        for (ConcurrentHashMap<String, Integer> m : map) {
            System.out.print("[" + m.size() + "]");
        }
        System.out.println();
    }

    @Override
    public void memberAdded(Member member) {
//        cluster.getCluster().pause();
        printMapSize();
        Result clusterInformation = cluster.ask(member, new Request<RingMap>() {
            @Override
            public void run(RingMap service, OperationContext ctx) {
                Map<String, Number> hashMap = new HashMap();
                int size = service.cluster.getCluster().getMembers().size();
                hashMap.put("memberCount", size);
                hashMap.put("startTime", service.cluster.startTime());
                ctx.reply(hashMap);
            }
        }).join();

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
    }

    private void joinCluster(Member oneMemberOfCluster) {
        Result result = cluster.ask(oneMemberOfCluster, (service, ctx) -> {
            ctx.reply(service.getRing());
        }).join();
        ConcurrentHashMap<String, Integer>[] oldBuckets = map;
        if (result.isSucceeded()) {
            ConsistentHashRing remoteRing = (ConsistentHashRing) result.getData();
            ConsistentHashRing newRing;
            if (remoteRing.getMembers().contains(localMember)) {
                ring = remoteRing.removeNode(localMember);
                newRing = remoteRing;
            } else {
                ring = remoteRing;
                newRing = remoteRing.addNode(localMember);
            }
            // we don't care about the old entries because the old ring doesn't have this local member so all operations will be remote.
            // the old entries will be added to the cluster when the new ring is set.
            changeRing(newRing).thenAccept(x -> {
                        // maybe we can parallelize this operation in order to make it fast
                        Arrays.stream(oldBuckets).forEach(map -> map.forEach(this::put));
                        LOGGER.info("Joined a cluster which has {} members and {} buckets.", ring.getMembers().size(), ring.getBuckets().size());
                    }
            );
        }
    }

    private CompletableFuture<Void> changeRing(ConsistentHashRing newRing) {
        System.out.println(ring.getBucketCount() + " -> " + newRing.getBucketCount());
        ConsistentHashRing oldRing = ring;
        ConcurrentHashMap<String, Integer>[] newMap = createEmptyMap();
        int[] newBucketIds = createBucketForRing(newRing);

        ArrayList<CompletableFuture> migrations = new ArrayList<>();
        newRing.getBuckets().forEach((range, members) -> {
            int start = oldRing.findClosest(range.start);
            int end = oldRing.findClosest(range.end - 1);

            if (members.contains(localMember)) {
                long cursor = range.start;

                int bckLoopEnd = end - start < 0 ? end + oldRing.getBucketCount() : end;
                for (int bucketId = start; bucketId <= bckLoopEnd; bucketId++) {
                    bucketId %= oldRing.getBucketCount();

                    ConsistentHashRing.Bucket oldBucket = oldRing.getBucket(bucketId);
                    List<Member> oldBucketMembers = oldBucket.members;

                    Member ownerMember;
                    if (oldBucketMembers.contains(localMember)) {
                        ownerMember = localMember;
                    } else {
                        int index = members.indexOf(localMember) % oldBucketMembers.size();
                        ownerMember = oldBucketMembers.get(index);
                    }

                    long queryStartToken = cursor;
                    long nextBucketToken = oldRing.getBucket(bucketId + 1).token;
                    long queryEndToken = (range.end - cursor > nextBucketToken - cursor) ? nextBucketToken : range.end;
                    cursor = queryEndToken;

                    System.out.println("asking entries [" + queryStartToken + " - " + queryEndToken + "] local " + ownerMember.equals(localMember));
                    CompletableFuture<Void> f = cluster.ask(ownerMember, (service, ctx) -> {
                        Map<String, Integer> moveEntries = new HashMap<>();

                        ConsistentHashRing serviceRing = service.ring;
                        int startBucket = serviceRing.findClosest(queryStartToken);
                        int endBucket = serviceRing.findClosest(queryEndToken);

                        int loopEnd = endBucket - startBucket < 0 ? endBucket + serviceRing.getBucketCount() : endBucket;

                        for (int bckId = startBucket; bckId < loopEnd; bckId++) {
                            bckId %= serviceRing.getBucketCount();
                            Map<String, Integer> partition = service.getPartition(bckId);
                            if (partition != null) {
                                if (ring.getBucket(bckId + 1).token >= range.end) {
                                    moveEntries.putAll(partition);
                                } else {
                                    partition.forEach((key, value) -> {
                                        long hash = ring.hash(key);
                                        if (hash >= range.start && hash < range.end) {
                                            moveEntries.put(key, value);
                                        }
                                    });
                                }
                            }
                        }

                        service.dataWaitingForMigration.forEach((token, map) -> {
                            if (range.end == token.end && range.start == token.start) {
                                moveEntries.putAll(map);
                                // remove
                            } else if (range.end >= range.start && range.start <= token.end) {
                                Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
                                while (iterator.hasNext()) {
                                    Map.Entry<String, Integer> next = iterator.next();
                                    long entryToken = serviceRing.hash(next.getKey());
                                    if (entryToken >= range.start && entryToken <= range.end) {
                                        moveEntries.put(next.getKey(), next.getValue());
                                        iterator.remove();
                                    }
                                }
                            }
                        });

                        migrationListeners.forEach(l -> l.migrationStart(localMember));
                        System.out.println("moving " + moveEntries.size() + " entries [" + queryStartToken + " - " + queryEndToken + "]");
                        ctx.reply(moveEntries);

                    }).thenAccept(result -> {
                        if (result.isSucceeded()) {
                            Map<String, Integer> data = (Map) result.getData();
                            int startBucket = newRing.findClosest(range.start);
                            int nextBucket = newRing.findClosest(range.end);
                            if (startBucket == nextBucket) {
                                newMap[startBucket].putAll(data);
                            } else {
                                data.forEach((key, value) -> {
                                    int i = Arrays.binarySearch(newBucketIds, newRing.findBucketId(key));
                                    if (i >= 0) {
                                        Map<String, Integer> partition = newMap[i];
                                        partition.put(key, value);
                                    }
                                });
                            }

                            if (!ownerMember.equals(localMember))
                                LOGGER.info("{} elements in token[{} - {}] moved from {} to {}", data.size(), queryStartToken, queryEndToken, ownerMember, localMember);
                        } else {
                            // TODO: try again until we the member removed from the cluster
                        }
                    });
                    migrations.add(f);
                }


            } else {
                for (int bucketId = start; bucketId < end; bucketId++) {
                    Map<String, Integer> partition = getPartition(bucketId);

                    HashMap<String, Integer> migrate = new HashMap<>();
                    for (Map.Entry<String, Integer> entry : partition.entrySet()) {
                        long hash = ConsistentHashRing.hash(entry.getKey());
                        if (!(hash < range.end && range.start <= hash)) {
                            migrate.put(entry.getKey(), entry.getValue());
                        }
                    }

                    // we don't remove the old entries because
                    // the new member will request the entries and remove them via migration request,
                    // so it allows us to avoid the requirement for consensus between nodes when changing ring.
                    dataWaitingForMigration.put(range, migrate);
                }
            }

        });

        // resume when all migrations completed
        return CompletableFuture.allOf(migrations.toArray(new CompletableFuture[migrations.size()]))
                .thenRun(() -> {
                    LOGGER.debug("{} migration completed.  New ring has {} buckets in member {}",
                            migrations.size(), newRing.getBuckets().size(), localMember);
                    migrationListeners.forEach(l -> l.migrationEnd(localMember));
                    bucketIds = newBucketIds;
                    map = newMap;
                    ring = newRing;
                    printMapSize();
                });
    }

    private void addMember(Member member) {
        ConsistentHashRing newRing = ring.addNode(member);
        changeRing(newRing).
                thenAccept(x -> cluster.getCluster().resume());
    }

    Map<String, Integer> getPartition(int bucketId) {
        int i = Arrays.binarySearch(bucketIds, bucketId);
        return i >= 0 ? map[i] : null;
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
    public void onClose() {
        Arrays.stream(map).forEach(x -> x.clear());
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

        CompletableFuture<Result>[] stages = new CompletableFuture[bucket.members.size()];
        int idx = 0;
        for (Member next : bucket.members) {
            if (next.equals(localMember)) {
                putLocal(key, val);
            } else {
                stages[idx++] = cluster.ask(next, new PutMapOperation(key, val));
            }
        }

        if (idx == 0)
            return CompletableFuture.completedFuture(null);

        // We should use eventual consistency here, that said we wait quorum of servers to perform the operation.
        return CompletableFuture.allOf(stages);
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
        Request<RingMap> bytes = (service, ctx) -> ctx.reply(service.getLocalSize());
        Map<Member, CompletableFuture<Result>> resultMap = cluster.askAllMembers(bytes);

        Map<Member, Integer> m = new ConcurrentHashMap<>(cluster.getCluster().getMembers().size());
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
        Map<String, Integer> partition = getPartition(ring.findBucketId(key));
        if (partition == null) {
            LOGGER.error("Discarded put request for key {} because node doesn't own that token.", key);
        } else {
            partition.put(key, value);
        }
    }
}