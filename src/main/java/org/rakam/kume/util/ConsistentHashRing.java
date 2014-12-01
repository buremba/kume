package org.rakam.kume.util;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.rakam.kume.Member;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/11/14 17:17.
 * This class is immutable.
 */
public class ConsistentHashRing {
    private final int bucketPerNode;
    final Bucket[] buckets;
    private static final HashFunction hashFunction = Hashing.murmur3_128();
    private final int replicationFactor;

    public ConsistentHashRing(Collection<Member> members, int bucketPerNode, int replicationFactor) {
        this.bucketPerNode = bucketPerNode;
        this.replicationFactor = replicationFactor;

        // todo: find a way to construct buckets without adding elements one by one.
        Iterator<Member> iterator = members.iterator();
        if(iterator.hasNext()) {
            Bucket[] list = findBucketListForNewNode(iterator.next(), bucketPerNode, replicationFactor, null);
            while(iterator.hasNext()) {
                list = findBucketListForNewNode(iterator.next(), bucketPerNode, replicationFactor, list);
            }
            buckets = list;
        }else {
            throw new IllegalArgumentException("member list must have at least one member");
        }
    }

    public Map<TokenRange, List<Member>> getBuckets() {
        HashMap<TokenRange, List<Member>> map = new HashMap<>();
        for (int i = 0; i < buckets.length; i++) {
            Bucket start = buckets[i];
            map.put(new TokenRange(i, start.token, buckets[ringPos(i+1)].token), Collections.unmodifiableList(start.members));
        }
        return map;
    }

    public Bucket getBucket(int i) {
        return buckets[i];
    }

    protected ConsistentHashRing(Bucket[] buckets, int bucketPerNode, int replicationFactor) {
        this.bucketPerNode = bucketPerNode;
        this.buckets = buckets;
        this.replicationFactor = replicationFactor;
    }

    public int findClosest(long l) {
        int low = 0;
        int high = buckets.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            if (buckets[mid].token < l)
                low = mid + 1;
            else if (buckets[mid].token > l)
                high = mid - 1;
            else
                return mid;
        }

        return l > 0 ? high : low;
    }

    public long hash(String hash) {
       return hashFunction.hashString(hash, Charset.forName("UTF-8")).asLong();
    }

    public int ringPos(int i) {
        return (i % buckets.length) + (i < 0 ? buckets.length : 0);
    }

    public double getTotalRingRange(Member member) {
        double total = 0;
        for (int i = 0; i < buckets.length; i++) {
            Bucket current = buckets[i];
            if(current.members.contains(member)) {
                if(i == buckets.length-1) {
                    total += Math.abs((Long.MAX_VALUE-current.token)/2)/(Long.MAX_VALUE/100.0);
                }else {
                    total += Math.abs((buckets[i+1].token-current.token)/2)/(Long.MAX_VALUE/100.0);
                }

            }
        }
        return total;
    }

    private static Bucket[] findBucketListForNewNode(Member member, int bucketPerNode, int replicationFactor, Bucket[] buckets) {
        if (buckets == null) {
            Bucket[] newBucketList = new Bucket[bucketPerNode];

            long token = Long.MAX_VALUE / (bucketPerNode/2);
            for (int i = 0; i < bucketPerNode; i++) {
                long t = Long.MIN_VALUE + (token * i);
                ArrayList<Member> members = new ArrayList<>(1);
                members.add(member);
                newBucketList[i] = new Bucket(members, t);
            }
            return newBucketList;
        }

        Bucket[] newBucketList = Arrays.copyOf(buckets, buckets.length + bucketPerNode);

        TokenGapPair[] tuples = new TokenGapPair[buckets.length];
        for (int i = 0; i < buckets.length; i++) {
            long nextBucketPos = (i+1 == buckets.length) ? Long.MAX_VALUE : buckets[i + 1].token;
            long gap = buckets[i].token - nextBucketPos;
            tuples[i] = new TokenGapPair(buckets[i].token, Math.abs(gap));
        }

        Arrays.sort(tuples, (o1, o2) -> (o2.gap > o1.gap) ? 1 : ((o2.gap < o1.gap) ? -1 : 0));
        for (int idx = 0; idx < bucketPerNode; idx++) {
            // todo: check the gap of next tuple and divide the current one if the gap > nextGap*2
            TokenGapPair current = tuples[idx];

            HashSet<Member> members = new HashSet<>();
            members.add(member);
            for (int i = 0, x = idx; i < buckets.length; i++) {
                for (Member member1 : buckets[x++ % buckets.length].members) {
                    members.add(member1);
                    if(members.size()==replicationFactor)
                        break;
                }
            }
            Bucket element = new Bucket(new ArrayList(members), current.token + current.gap / 2);
            newBucketList[idx + buckets.length] = element;
        }
        Arrays.sort(newBucketList, (o1, o2) -> o2.token > o1.token ? -1 : (o2.token == o1.token ? 0 : 1));

        return newBucketList;
    }

    public ConsistentHashRing addNode(Member member) {
        Bucket[] buckets = findBucketListForNewNode(member, bucketPerNode, replicationFactor, this.buckets);
        return new ConsistentHashRing(buckets, bucketPerNode, replicationFactor);
    }

    public ConsistentHashRing removeNode(Member member) {
        List<Bucket> result = Lists.newArrayList(this.buckets);

        Iterator<Bucket> iterator = result.iterator();
        while(iterator.hasNext()) {
            Bucket next = iterator.next();
            if (next.members.contains(member))
                iterator.remove();
        }

        Bucket[] buckets = new Bucket[result.size()];
        result.toArray(buckets);
        return new ConsistentHashRing(buckets, bucketPerNode, replicationFactor);
    }

    public Bucket findBucket(String key) {
        return buckets[findClosest(hash(key))];
    }

    public int findBucketId(String key) {
        return findClosest(hash(key));
    }


    public static class Bucket {
        public long token;
        public ArrayList<Member> members;

        public Bucket(ArrayList<Member> members, long token) {
            this.members = members;
            this.token = token;
        }
    }

    private static class TokenGapPair {
        public final long token;
        public final long gap;

        public TokenGapPair(long token, long gap) {
            this.token = token;
            this.gap = gap;
        }
    }

    public class TokenRange {
        public final long start;
        public final long end;
        public final int id;

        private TokenRange(int id, long start, long end) {
            this.id = id;
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TokenRange)) return false;

            TokenRange that = (TokenRange) o;

            if (end != that.end) return false;
            if (start != that.start) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (end ^ (end >>> 32));
            return result;
        }
    }
}
