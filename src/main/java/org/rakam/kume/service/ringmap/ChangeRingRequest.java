package org.rakam.kume.service.ringmap;

import org.rakam.kume.Member;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;
import org.rakam.kume.util.ConsistentHashRing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.rakam.kume.util.ConsistentHashRing.TokenRange;
import static org.rakam.kume.util.ConsistentHashRing.isTokenBetween;

/**
* Created by buremba <Burak Emre Kabakcı> on 17/12/14 00:58.
*/
class ChangeRingRequest implements Request<RingMap> {
    private final long queryStartToken;
    private final long queryEndToken;
    private final ConsistentHashRing oldRing;

    public ChangeRingRequest(long queryStartToken, long queryEndToken, ConsistentHashRing oldRing) {
        this.queryStartToken = queryStartToken;
        this.queryEndToken = queryEndToken;
        this.oldRing = oldRing;
    }

    @Override
    public void run(RingMap service, OperationContext ctx) {
        synchronized (service.cluster) {
            Map<String, Integer> moveEntries = new HashMap<>();

            ConsistentHashRing serviceRing = service.getRing();
            int startBucket = serviceRing.findBucketFromToken(queryStartToken);
            int endBucket = serviceRing.findBucketFromToken(queryEndToken);

            int loopEnd = endBucket - startBucket < 0 ? endBucket + serviceRing.getBucketCount() : endBucket;

            for (int bckIdz = startBucket; bckIdz <= loopEnd; bckIdz++) {
                int bckId = bckIdz % serviceRing.getBucketCount();
                Map<String, Integer> partition = service.getPartition(bckId);
                if (partition != null) {
                    // for the current ring implementation, most of the time
                    // queryEndToken == serviceRing.getBucket(bckId + 1).token
                    // so we can't directly add partition to moveEntries. FIXME!HOUSTON

                    int i = bckId + 1;
                    long token = serviceRing.getBucket(i).token;
                    if (i == serviceRing.getBucketCount() ? token > queryEndToken : token < queryEndToken) {
                        moveEntries.putAll(partition);
                    } else {
                        partition.forEach((key, value) -> {
                            long hash = serviceRing.hash(key);
                            if (isTokenBetween(hash, queryStartToken, queryEndToken)) {
                                moveEntries.put(key, value);
                            }
                        });
                    }
                } else {
                    // it seems partition table is changed (most probably updated before this request is processed)
                    // so the old items must be in dataWaitingForMigration.

                    Iterator<Map.Entry<TokenRange, Map<String, Integer>>> it;
                    it = service.dataWaitingForMigration.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<TokenRange, Map<String, Integer>> next = it.next();
                        TokenRange token = next.getKey();
                        Map<String, Integer> map = next.getValue();

                        boolean startTokenIn = isTokenBetween(token.start, queryStartToken, queryEndToken);
                        boolean endTokenIn = isTokenBetween(token.end, queryStartToken, queryEndToken);

                        if (startTokenIn && endTokenIn) {
                            moveEntries.putAll(map);
                            it.remove();
                        } else if (startTokenIn || endTokenIn) {
                            Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, Integer> n = iterator.next();
                                long entryToken = serviceRing.hash(n.getKey());
                                if (isTokenBetween(entryToken, queryStartToken, queryEndToken)) {
                                    moveEntries.put(n.getKey(), n.getValue());
                                    iterator.remove();
                                }
                            }
                        }
                    }
                }
            }

//            if(moveEntries.size() == 0) {
//
//                boolean i1 = Arrays.binarySearch( service.bucketIds, startBucket) >= 0;
//                boolean i2 = Arrays.binarySearch( service.bucketIds, endBucket) >= 0;
//
//                if(!i1 && !i2) {
//                    System.out.println("new i don't have that range");
//                }
//
//                int closestSb = oldRing.findBucketFromToken(queryStartToken);
//                int closestEb = oldRing.findBucketFromToken(queryEndToken);
//                int[] bucketForRing = service.createBucketForRing(oldRing);
//
//                boolean i3 = Arrays.binarySearch(bucketForRing, closestSb) >= 0;
//                boolean i4 = Arrays.binarySearch(bucketForRing, closestEb) >= 0;
//
//                if(!i3 && !i4) {
//                    System.out.println("old i don't have that range");
//                }
//            }

            Member sender = ctx.getSender();
            if (sender == null || !sender.equals(service.cluster.getCluster().getLocalMember()))
                RingMap.LOGGER.debug("moving {} entries [{}, {}] to {}", moveEntries.size(), queryStartToken, queryEndToken, sender);
            else
                RingMap.LOGGER.debug("moving {} entries [{}, {}] to local", moveEntries.size(), queryStartToken, queryEndToken);

            ctx.reply(moveEntries);
        }
    }
}
