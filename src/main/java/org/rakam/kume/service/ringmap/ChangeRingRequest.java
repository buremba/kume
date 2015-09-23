package org.rakam.kume.service.ringmap;

import org.rakam.kume.Member;
import org.rakam.kume.transport.OperationContext;
import org.rakam.kume.transport.Request;
import org.rakam.kume.transport.serialization.Serializer;
import org.rakam.kume.util.ConsistentHashRing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Map.Entry;
import static org.rakam.kume.util.ConsistentHashRing.TokenRange;
import static org.rakam.kume.util.ConsistentHashRing.hash;
import static org.rakam.kume.util.ConsistentHashRing.isTokenBetween;

/**
* Created by buremba <Burak Emre Kabakcı> on 17/12/14 00:58.
*/
class ChangeRingRequest<K, V> implements Request<RingMap, Map<K, V>> {
    private final long queryStartToken;
    private final long queryEndToken;
    private ConsistentHashRing oldRing;

    public ChangeRingRequest(long queryStartToken, long queryEndToken, ConsistentHashRing oldRing) {
        this.queryStartToken = queryStartToken;
        this.queryEndToken = queryEndToken;
        this.oldRing = oldRing;
    }

    public ChangeRingRequest(long queryStartToken, long queryEndToken) {
        this.queryStartToken = queryStartToken;
        this.queryEndToken = queryEndToken;
    }

    @Override
    public void run(RingMap service, OperationContext ctx) {
//        synchronized (service) {
            Map<K, V> moveEntries = new HashMap<>();

            ConsistentHashRing serviceRing = service.getRing();
            int startBucket = serviceRing.findBucketIdFromToken(queryStartToken);
            int endBucket = serviceRing.findBucketIdFromToken(queryEndToken);

            int loopEnd = endBucket - startBucket < 0 ? endBucket + serviceRing.getBucketCount() : endBucket;

            for (int bckIdz = startBucket; bckIdz <= loopEnd; bckIdz++) {
                int bckId = bckIdz % serviceRing.getBucketCount();
                Map<K, V> partition = service.getBucket(bckId);
                if (partition != null) {
                    long endToken = serviceRing.getBucket(bckId + 1).token-1;
                    long startToken = serviceRing.getBucket(bckId).token;

                    boolean partitionBetweenToken = isTokenBetween(startToken, queryStartToken, queryStartToken);
                    partitionBetweenToken &= isTokenBetween(endToken, queryStartToken, queryStartToken);

                    if (partitionBetweenToken) {
                        moveEntries.putAll(partition);
                    } else {
                        partition.forEach((key, value) -> {
                            if (isTokenBetween(hash(key), queryStartToken, queryEndToken)) {
                                moveEntries.put(key, value);
                            }
                        });
                    }
                } else {
                    // it seems partition table is changed (most probably updated before this request is processed)
                    // so the old items must be in dataWaitingForMigration.

                    Iterator<Entry<TokenRange, Map>> it = service.dataWaitingForMigration.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<TokenRange, Map> next = it.next();
                        TokenRange token = next.getKey();
                        Map map = next.getValue();

                        boolean startTokenIn = isTokenBetween(token.start, queryStartToken, queryEndToken);
                        boolean endTokenIn = isTokenBetween(token.end, queryStartToken, queryEndToken);

                        if (startTokenIn && endTokenIn) {
                            moveEntries.putAll(map);
                            it.remove();
                        } else if (startTokenIn || endTokenIn) {
                            Iterator<Entry<K, V>> iterator = map.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Entry<K, V> n = iterator.next();
                                long entryToken = hash(Serializer.toByteArray(n.getKey()));
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
//                boolean i1 = Arrays.binarySearch(service.bucketIds, startBucket) >= 0;
//                boolean i2 = Arrays.binarySearch( service.bucketIds, endBucket) >= 0;
//
//                if(!i1 && !i2) {
//                    System.out.println("new i don't have that range");
//                }
//
//                int closestSb = oldRing.findBucketIdFromToken(queryStartToken);
//                int closestEb = oldRing.findBucketIdFromToken(queryEndToken);
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
            if (sender == null || !sender.equals(service.getContext().getCluster().getLocalMember()))
                RingMap.LOGGER.debug("moving {} entries [{}, {}] to {}", moveEntries.size(), queryStartToken, queryEndToken, sender);
            else
                RingMap.LOGGER.debug("moving {} entries [{}, {}] to local", moveEntries.size(), queryStartToken, queryEndToken);

            ctx.reply(moveEntries);
        }
//    }
}
