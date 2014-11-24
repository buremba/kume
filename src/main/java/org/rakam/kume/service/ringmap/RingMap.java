package org.rakam.kume.service.ringmap;

import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.MembershipListener;
import org.rakam.kume.Operation;
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
    Cluster cluster;

    private final Member localMember;
    private boolean paused;

    public RingMap(Cluster cluster) {
        this.cluster = cluster;

        cluster.addMembershipListener(this);
        localMember = cluster.getLocalMember();
    }

    @Override
    public void memberAdded(Member member) {
        cluster.pause();

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
            cluster.resume();
        }

    }

    @Override
    public void memberRemoved(Member member) {
        cluster.pause();

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
            cluster.resume();
        }
    }

    @Override
    public void handleOperation(Operation operation) {
        try {
            if(operation instanceof MapOperation) {
                MapOperation mapOperation = (MapOperation) operation;
                mapOperation.setMap(map);
                mapOperation.run();
            }else {
                LOGGER.warn("Discarded unidentified message", operation);
            }
        } catch (Exception e) {
            LOGGER.error("Couldn't execute operation", e);
            e.printStackTrace();
        }
    }

    @Override
    public Object handleRequest(Request request) {
        if(request instanceof MapRequest) {
            MapRequest mapOperation = (MapRequest) request;
            mapOperation.setMap(map);
            return mapOperation.run();
        }else {
            throw new IllegalArgumentException("The Request must be an instance of MapRequests");
        }
    }

    @Override
    public void onStart() {
        ring = new ConsistentHashRing(cluster.getClusterMembers(), 8);
    }

    @Override
    public void onClose() {
        map.clear();
        paused = true;
    }

    public void put(String key, Integer val) {
        Collection<Member> nodes = ring.findNode(key);
        for (Member next : nodes) {
            if (next.equals(localMember)) {
                map.put(key, val);
            } else {
                cluster.send(next.getId(), new PutMapOperation(key, val));
            }
        }
    }

    public Map<String, Integer> localMap() {
        return Collections.unmodifiableMap(map);
    }


    public CompletionStage<Map<Member, Integer>> size() {
        Map<Member, CompletionStage<Result>> resultMap = cluster.sendAllMembers(new MapRequest<Integer>() {
            @Override
            public Integer run() {
                return map.size();
            }
        });

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


    public static class PutMapOperation extends MapOperation {
        String key;
        Integer value;

        public PutMapOperation(String key, Integer value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void execute() {
            map.put(key, value);
        }
    }
    public static class PutAllMapOperation extends MapOperation {
        Map<String, Integer> addAll;

        public PutAllMapOperation(Map<String, Integer> map) {
            this.addAll = map;
        }

        @Override
        public void execute() {
            map.putAll(this.addAll);
        }
    }
}
