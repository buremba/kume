package org.rakam.kume.service.ringmap;

import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.MembershipListener;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;
import org.rakam.kume.Result;
import org.rakam.kume.service.Service;
import org.rakam.kume.util.ConsistentHashRing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class RingMap implements MembershipListener, Service {
    final static int SERVICE_ID = 0;
    final static Logger LOGGER = LoggerFactory.getLogger(RingMap.class);

    ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
    ConsistentHashRing ring;
    Cluster.ServiceContext<RingMap> cluster;

    private final Member localMember;
    private boolean paused;

    public RingMap(Cluster.ServiceContext cluster) {
        this.cluster = cluster;

        cluster.getCluster().addMembershipListener(this);
        localMember = cluster.getCluster().getLocalMember();
    }

    @Override
    public void memberAdded(Member member) {
        cluster.getCluster().pause();

        try {
            ring = ring.addNode(member);

            if (!ring.isSibling(member, localMember))
                return;

            int migrationSize = 0;
            if (member.equals(localMember))
                return;

            HashMap<String, Integer> map = new HashMap();
            for (Map.Entry<String, Integer> entry : this.map.entrySet()) {
                String key = entry.getKey();

                Collection<Member> ownedNodes = ring.findNode(key);
                if(!ownedNodes.contains(localMember))
                    map.remove(key);

                if(!ownedNodes.contains(localMember))
                    continue;

                Integer val = entry.getValue();
                map.put(key, val);
                this.map.remove(key);
                migrationSize++;
            }

            if(map.size()>0) {
                LOGGER.info("Migrating " + migrationSize + " items from " + member);

                final int finalMigrationSize = migrationSize;
                cluster.send(member.getId(), new PutAllMapOperation(map)).thenAccept(result ->
                        LOGGER.info("Successfully completed migration. {} elements moved to new member {}", finalMigrationSize, member));
            }
        } finally {
            cluster.getCluster().resume();
        }

    }

    @Override
    public void memberRemoved(Member member) {
        cluster.getCluster().pause();

        try {
            ConsistentHashRing oldRing = ring;
            ring = ring.removeNode(member);

            List<ConsistentHashRing.Node> oldBuckets = oldRing.getBuckets();
            for (int i = 0; i < oldBuckets.size(); i++) {
                ConsistentHashRing.Node bucket = oldBuckets.get(i);
                if(!bucket.member.equals(localMember))
                    continue;

                Set<Member> oldNextBackups = oldRing.findNextBackups(i, 2);
                if(oldNextBackups.contains(member)) {
                    Set<Member> nextBackups = ring.findNextBackups(i, 2);
                    nextBackups.removeAll(oldNextBackups);
                    Member next = nextBackups.iterator().next();
                    if(map.size()>0) {
                        LOGGER.info("Migrating " + map.size() + " items from " + member);
                        cluster.send(next.getId(), new PutAllMapOperation(map))
                            .thenAccept(result -> {
                                LOGGER.info("Successfully completed migration. {} elements moved to member {}", map.size(), member);
                            });
                    }
                }
            }
        } finally {
            cluster.getCluster().resume();
        }
    }

    @Override
    public void handle(OperationContext ctx, Object request) {
        //
    }

    @Override
    public void onStart() {
        ring = new ConsistentHashRing(cluster.getCluster().getClusterMembers(), 8);
    }

    @Override
    public void onClose() {
        map.clear();
        paused = true;
    }

    public CompletionStage<Boolean> put(String key, Integer val) {
        Collection<Member> nodes = ring.findNode(key);

        CompletableFuture<Boolean> f = new CompletableFuture<>();

        CompletionStage[] stages = new CompletionStage[nodes.size()];
        int idx = 0;
        final int[] resultArr = {0, 0};
        for (Member next : nodes) {
            if (next.equals(localMember)) {
                map.put(key, val);
            } else {
                stages[idx] = cluster.send(next.getId(), new PutMapOperation(key, val)).thenAccept(x -> {
                    if(x.isSucceeded()) {
                        resultArr[0]++;
                    }else {
                        resultArr[1]++;
                    }
                    if(resultArr[0] > stages.length/2) {
                        f.complete(true);
                    }else
                    if(resultArr[1] > stages.length/2) {
                        f.complete(false);
                    }
                });
            }
            idx++;
        }

        return f;
    }

    public CompletionStage<Result> get(String key) {
        Collection<Member> nodes = ring.findNode(key);
        if(nodes.contains(localMember)) {
            return CompletableFuture.completedFuture(new Result(map.get(key)));
        }

        return cluster.send(nodes.iterator().next().getId(), (service, ctx) -> service.map.get(key));
    }

    public Map<String, Integer> localMap() {
        return Collections.unmodifiableMap(map);
    }


    public CompletionStage<Map<Member, Integer>> size() {
        Map<Member, CompletionStage<Result>> resultMap = cluster.sendAllMembers((service, ctx) -> ctx.reply(map.size()));

        Map<Member, Integer> m = new ConcurrentHashMap<>(map.size());
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


    public static class PutMapOperation implements Request<RingMap> {
        String key;
        Integer value;

        public PutMapOperation(String key, Integer value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void run(RingMap service, OperationContext ctx) {
            service.map.put(key, value);
        }
    }
    public static class PutAllMapOperation implements Request<RingMap> {
        Map<String, Integer> addAll;

        public PutAllMapOperation(Map<String, Integer> map) {
            this.addAll = map;
        }

        @Override
        public void run(RingMap service, OperationContext ctx) {
            service.map.putAll(this.addAll);
        }

    }
}
