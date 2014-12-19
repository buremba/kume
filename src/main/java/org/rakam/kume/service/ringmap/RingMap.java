package org.rakam.kume.service.ringmap;

import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.MembershipListener;
import org.rakam.kume.MigrationListener;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;
import org.rakam.kume.Result;
import org.rakam.kume.service.PausableService;
import org.rakam.kume.util.ConsistentHashRing;
import org.rakam.kume.util.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.Entry;
import static org.rakam.kume.util.ConsistentHashRing.TokenRange;

public class RingMap extends PausableService implements MembershipListener {
    final static Logger LOGGER = LoggerFactory.getLogger(RingMap.class);

    ConcurrentHashMap<String, Integer>[] map;
    private int[] bucketIds;
    private ConsistentHashRing ring;
    Cluster.ServiceContext<RingMap> cluster;

    private final Member localMember;
    private final int replicationFactor;
    private LinkedList<MigrationListener> migrationListeners = new LinkedList<>();
    Map<TokenRange, Map<String, Integer>> dataWaitingForMigration = new HashMap<>();

    public RingMap(Cluster.ServiceContext cluster, int replicationFactor) {
        this.cluster = cluster;
        this.replicationFactor = replicationFactor;

        cluster.getCluster().addMembershipListener(this);
        localMember = cluster.getCluster().getLocalMember();

        ConsistentHashRing newRing = new ConsistentHashRing(cluster.getCluster().getMembers(), 8, replicationFactor);
        ring = newRing;
        bucketIds = createBucketForRing(newRing);
        map = createEmptyMap(ring);
    }

    protected int[] createBucketForRing(ConsistentHashRing ring) {
        return ring.getBuckets().entrySet().stream()
                .filter(entry -> entry.getValue().contains(localMember))
                .mapToInt(entry -> entry.getKey().id).sorted().toArray();
    }

    private ConcurrentHashMap[] createEmptyMap(ConsistentHashRing ring) {
        long count = ring.getBuckets().entrySet().stream().filter(x -> x.getValue().contains(localMember)).count();
        return Stream.generate(() -> new ConcurrentHashMap<>()).limit(count)
                .toArray(value -> new ConcurrentHashMap[(int) count]);
    }

    private void printMapSize() {
        System.out.print("ownedBuckets[" + map.length + "]: ");
        int i = 0;
        for (ConcurrentHashMap<String, Integer> m : map) {
            System.out.print("[" + m.size() + "]");
            i += m.size();
        }
        System.out.println(" = " + i);
    }

    @Override
    public void memberAdded(Member member) {
        if (ring.getMembers().contains(member)) {
            // it means we joined this cluster already
            // via requesting ring from an existing node.
            return;
        }

        printMapSize();
        Member sourceMember = localMember;
        Result clusterInformation = cluster.ask(member, (service, ctx) -> {
            Map<String, Number> hashMap = new HashMap();
            Set<Member> m = service.cluster.getCluster().getMembers();
            hashMap.put("memberCount", m.contains(sourceMember) ? m.size() - 1 : m.size());
            hashMap.put("startTime", service.cluster.startTime());
            ctx.reply(hashMap);
        }).join();

        if (clusterInformation.isSucceeded()) {
            Map<String, Number> data = (Map<String, Number>) clusterInformation.getData();
            int myClusterSize = cluster.getCluster().getMembers().size() - 1;
            int otherClusterSize = data.get("memberCount").intValue();
            long startTime = data.get("startTime").longValue();
            if (otherClusterSize > myClusterSize) {
                joinCluster(member, otherClusterSize);
            } else if (otherClusterSize == myClusterSize && startTime < cluster.startTime()) {
                joinCluster(member, otherClusterSize);
            } else {
                addMember(member);
            }
        }
    }

    private void joinCluster(Member oneMemberOfCluster, int otherClusterSize) {
        LOGGER.debug("Joining a cluster of {} nodes.", otherClusterSize);

        Result result = cluster.ask(oneMemberOfCluster, (service, ctx) -> ctx.reply(service.getRing())).join();

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
//                        Arrays.stream(oldBuckets).forEach(map -> map.forEach(this::put));
                        Set<Member> members = ring.getMembers();
                        LOGGER.info("Joined a cluster which has {} members {}.", members.size(), members);
                    }
            ).join();
        } else {
            LOGGER.error("change join cluster", result);
        }
    }

    private CompletableFuture<Void> changeRing(ConsistentHashRing newRing) {
        ConsistentHashRing oldRing = ring;
        ConcurrentHashMap<String, Integer>[] newMap = createEmptyMap(newRing);
        int[] newBucketIds = createBucketForRing(newRing);

        ArrayList<CompletableFuture> migrations = new ArrayList<>();
        for (Entry<TokenRange, List<Member>> entry : newRing.getBuckets().entrySet()) {
            TokenRange range = entry.getKey();
            List<Member> members = entry.getValue();

            int start = oldRing.findBucketFromToken(range.start);
            int end = oldRing.findBucketFromToken(range.end - 1);
            if(end - start < 0) end = end + oldRing.getBucketCount();

            if (members.contains(localMember)) {
                long cursor = range.start;

                for (int bucketId = start; bucketId <= end; bucketId++) {
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
                    long queryEndToken = (range.end - cursor > nextBucketToken - cursor) ? nextBucketToken-1 : range.end-1;
                    cursor = queryEndToken;

                    boolean isLocalOp = ownerMember.equals(localMember);
                    if (!isLocalOp)
                        LOGGER.debug("asking entries [{}, {}] from {}", queryStartToken, ownerMember);
                    else
                        LOGGER.trace("asking entries [{}, {}] from {}", queryStartToken, ownerMember);

                    CompletableFuture<Void> f = cluster.ask(ownerMember, new ChangeRingRequest(queryStartToken, queryEndToken, oldRing))
                            .thenAccept(result -> {
                                if (result.isSucceeded()) {
                                    Map<String, Integer> data = (Map) result.getData();
                                    int startBucket = newRing.findBucketFromToken(queryStartToken);
                                    int nextBucket = newRing.findBucketFromToken(queryEndToken);
                                    if (startBucket == nextBucket) {
                                        try {
                                            newMap[Arrays.binarySearch(newBucketIds, startBucket)].putAll(data);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    } else {
                                        data.forEach((key, value) -> {
                                            int i = Arrays.binarySearch(newBucketIds, newRing.findBucketId(key));
                                            if (i >= 0) {
                                                Map<String, Integer> partition = newMap[i];
                                                partition.put(key, value);
                                            }
                                        });
                                    }
                                    ConcurrentHashMap<String, Integer>[] c = newMap;
                                    int[] d = newBucketIds;
                                    ConsistentHashRing r = newRing;

                                    if (!ownerMember.equals(localMember))
                                        LOGGER.debug("{} elements in token[{} - {}] moved from {} to {}", data.size(), queryStartToken, queryEndToken, ownerMember, localMember);
                                } else {
                                    // TODO: try again until we the member removed from the cluster
                                    LOGGER.error("change ring error", result);
                                }
                            });
                    migrations.add(f);
                }

            } else {
                for (int bucketId = start; bucketId <= end; bucketId++) {
                    // we don't remove the old entries because
                    // the new member will request the entries and remove them via migration request,
                    // so it allows us to avoid the requirement for consensus between nodes when changing ring.
                    dataWaitingForMigration.put(range, getPartition(bucketId));
                }
            }
        }

        if (migrations.size() > 0) {
            migrationListeners.forEach(l -> Throwables.propagate(() -> l.migrationStart(localMember)));
        }

        // resume when all migrations completed
        return CompletableFuture.allOf(migrations.toArray(new CompletableFuture[migrations.size()]))
                .thenRun(() -> {
                    LOGGER.debug("{} migration completed.  New ring has {} buckets in member {}",
                            migrations.size(), newRing.getBuckets().size(), localMember);
                    synchronized (cluster) {
                        bucketIds = newBucketIds;
                        map = newMap;
                        ring = newRing;
                    }
                    migrationListeners.forEach(l -> Throwables.propagate(() -> l.migrationEnd(localMember)));
                    printMapSize();
                });
    }

    private void addMember(Member member) {
        LOGGER.debug("Adding member {} to existing cluster of {} nodes.", member, cluster.getCluster().getMembers().size());

        ConsistentHashRing newRing = ring.addNode(member);
        changeRing(newRing).join();
    }

    Map<String, Integer> getPartition(int bucketId) {
        int i = Arrays.binarySearch(bucketIds, bucketId);
        return i >= 0 ? map[i] : null;
    }

    @Override
    public void memberRemoved(Member member) {
        if (isPaused()) {
            addQueueIfPaused(() -> memberRemoved(member));
        } else {
            changeRing(ring.removeNode(member)).thenAccept(x -> cluster.getCluster().resume());
        }
    }

    @Override
    public void onClose() {
        Arrays.stream(map).forEach(x -> x.clear());
    }

    public CompletableFuture<Void> putAll(Map<String, Integer> fromMap) {
        Map<Member, List<Entry<String, Integer>>> m = new HashMap<>();

        for (Entry<String, Integer> entry : fromMap.entrySet()) {
            for (Member member : ring.findBucket(entry.getKey()).members) {
                m.getOrDefault(member, new ArrayList()).add(entry);
            }
        }

        for (Entry<Member, List<Entry<String, Integer>>> entry : m.entrySet()) {
            cluster.send(entry.getKey(), new PutAllMapOperation(entry.getValue()));
        }
        CompletableFuture[] completableFutures = fromMap.entrySet().stream()
                .map(entry -> put(entry.getKey(), entry.getValue()))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(completableFutures);
    }

    public CompletableFuture<Void> put(String key, Integer val) {
        int bucketId = ring.findBucketId(key);
        ConsistentHashRing.Bucket bucket = ring.getBucket(bucketId);

        CompletableFuture<Result>[] stages = new CompletableFuture[bucket.members.size()];
        int idx = 0;
        for (Member next : bucket.members) {
            if (next.equals(localMember)) {
                putLocal(key, val);
                stages[idx++] = CompletableFuture.completedFuture(null);
            } else {
                stages[idx++] = cluster.ask(next, new PutMapOperation(key, val));
            }
        }

        // we should use eventual consistency here,
        // that said it should wait quorum of servers to complete this Future.
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

    void putLocal(String key, Integer value) {
        Map<String, Integer> partition = getPartition(ring.findBucketId(key));
        if (partition == null) {
            LOGGER.error("Discarded put request for key {} because node doesn't own that token.", key);
        } else {
            partition.put(key, value);
        }
    }
}