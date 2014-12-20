package org.rakam.kume.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.rakam.kume.Member;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/11/14 17:17.
 */
public class ConsistentHashRing {
    private final int bucketPerNode;
    private final Bucket[] buckets;
    private static final HashFunction hashFunction = Hashing.murmur3_128();
    private final int replicationFactor;


    public static boolean isTokenBetween(long hash, long start, long end) {
        if(start <= end) return hash >= start && hash <= end;
        // we're in the start point of ring
        else return hash > start || hash < end;
    }

    public ConsistentHashRing(Collection<Member> members, int bucketPerNode, int replicationFactor) {
        this.bucketPerNode = bucketPerNode;
        this.replicationFactor = replicationFactor;

        // todo: find a way to construct buckets without adding elements one by one.
        Iterator<Member> iterator = members.iterator();
        if (iterator.hasNext()) {
            Bucket[] list = findBucketListForNewNode(iterator.next(), null);
            while (iterator.hasNext()) {
                list = findBucketListForNewNode(iterator.next(), list);
            }
            buckets = list;
        } else {
            throw new IllegalArgumentException("member list must have at least one member");
        }
    }

    protected ConsistentHashRing(Bucket[] buckets, int bucketPerNode, int replicationFactor) {
        this.bucketPerNode = bucketPerNode;
        this.buckets = buckets;
        this.replicationFactor = replicationFactor;
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        getBuckets().forEach((range, members) -> {
            str.append("[" + range.start + "-" + range.end + ", " + members.size() + " members]");
        });
        return str.toString();
    }

    public Map<TokenRange, List<Member>> getBuckets() {
        return getBuckets(buckets);
    }

    private Map<TokenRange, List<Member>> getBuckets(Bucket[] buckets) {
        return IntStream.range(0, buckets.length)
                .mapToObj(i -> {
                    Bucket bucket = buckets[i];
                    TokenRange tokenRange = new TokenRange(i, bucket.token, getBucketFromRing(buckets, i + 1).token);
                    return new Tuple<>(tokenRange, bucket.members);
                }).collect(Collectors.toMap(Tuple::_1, Tuple::_2));
    }

    public Bucket getBucket(int i) {
        int length = buckets.length;
        if (i >= length)
            i = i % length;

        return buckets[i >= 0 ? i : i + length];
    }


    public int getBucketCount() {
        return buckets.length;
    }

    public int getMemberCount() {
        return buckets.length / bucketPerNode;
    }

    public TokenRange getBucketRange(int i) {
        return new TokenRange(i, buckets[i].token, getBucketFromRing(buckets, i + 1).token);
    }

    public int findBucketIdFromToken(long l) {
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

        return high;
    }

    public Bucket findBucketFromToken(long l) {
        return buckets[findBucketIdFromToken(l)];
    }


    public static long hash(String hash) {
        return hashFunction.hashString(hash, Charset.forName("UTF-8")).asLong();
    }

    public static long hash(byte[] hash) {
        return hashFunction.hashBytes(hash).asLong();
    }

    public static long hash(long hash) {
        return hashFunction.hashLong(hash).asLong();
    }

    public static long hash(int hash) {
        return hashFunction.hashInt(hash).asLong();
    }

    private Bucket getBucketFromRing(Bucket[] buckets, int i) {
        return buckets[(i % buckets.length) + (i < 0 ? buckets.length : 0)];
    }

    public double getTotalRingRange(Member member) {
        double total = 0;
        for (int i = 0; i < buckets.length; i++) {
            Bucket current = buckets[i];
            if (current.members.contains(member)) {
                if (i == buckets.length - 1) {
                    total += (((Long.MAX_VALUE - current.token) + (buckets[0].token - Long.MIN_VALUE)) / 2) / (Long.MAX_VALUE / 100.0);
                } else {
                    total += Math.abs((buckets[i + 1].token - current.token) / 2) / (Long.MAX_VALUE / 100.0);
                }

            }
        }
        return total;
    }

    private Bucket[] findBucketListForNewNode(Member member, Bucket[] buckets) {
        if (buckets == null) {
            long token = Long.MAX_VALUE / (bucketPerNode / 2);

            return IntStream.range(0, bucketPerNode).mapToObj(i -> {
                long t = Long.MIN_VALUE + (token * i);
                return new Bucket(Sets.newHashSet(member), t);
            }).toArray(Bucket[]::new);
        }

        Bucket[] newBucketList = Arrays.copyOf(buckets, buckets.length + bucketPerNode);


        // find the members who owns less data than other to use them as replica of new buckets
        Map<Member, Long> result = new HashMap<>();
        getBuckets(buckets)
                .entrySet().stream()
                .filter(x -> !x.getValue().contains(member))
                .forEach(x -> x.getValue().forEach(z -> result.merge(z, x.getKey().gap() / 2, Long::sum)));
        Set<Map.Entry<Member, Long>> memberSet = result.entrySet();

        // find the larger gaps to and divide them in order to create new buckets
        TokenRange[] tokens = getBuckets(buckets).entrySet().stream()
                .sorted((o1, o2) -> {
                    int compare = Long.compare(o2.getKey().gap(), o1.getKey().gap());
                    if (compare == 0) {
                        // we compare the nodes that own this bucket and choose the bucket
                        // that has members that owns minimum range on the ring.
                        long sum1 = o1.getValue().stream().mapToLong(x -> result.get(x)).sum();
                        long sum2 = o2.getValue().stream().mapToLong(x -> result.get(x)).sum();
                        compare = Long.compare(sum2, sum1);
                        if (compare == 0) {
                            // it's pointless but if such condition occurs we need
                            // a way that all nodes agree.
                            return Long.compare(o1.getKey().start, o2.getKey().start);
                        }
                    }
                    return compare;
                }).limit(8).map(x -> x.getKey()).toArray(TokenRange[]::new);

        for (int idx = 0; idx < tokens.length; idx++) {
            TokenRange current = tokens[idx];
            long gap = current.gap() / 2;

            HashSet<Member> members = new HashSet<>();
            members.add(member);

            IntStream.range(0, replicationFactor - 1).forEach(i -> {
                Map.Entry<Member, Long> m = memberSet.stream()
                        .sorted((x, y) -> Long.compare(x.getValue(), y.getValue())).findFirst().get();
                members.add(m.getKey());
                m.setValue(m.getValue() + gap);
            });

            Bucket element = new Bucket(members, current.start + gap);
            newBucketList[idx + buckets.length] = element;
        }
        Arrays.sort(newBucketList, (o1, o2) -> Long.compare(o1.token, o2.token));

        if (buckets.length / bucketPerNode < replicationFactor) {
            for (int i = 0; i < newBucketList.length; i++) {
                Bucket oldBucket = newBucketList[i];

                if (oldBucket.members.size() < replicationFactor) {
                    HashSet members = new HashSet(oldBucket.members);
                    members.add(member);
                    newBucketList[i] = new Bucket(members, newBucketList[i].token);
                }
            }
        }

        return newBucketList;
    }

    public ConsistentHashRing addNode(Member member) {
        if (getMembers().contains(member)) {
            return new ConsistentHashRing(buckets, bucketPerNode, replicationFactor);
        }

        Bucket[] buckets = findBucketListForNewNode(member, this.buckets);
        return new ConsistentHashRing(buckets, bucketPerNode, replicationFactor);
    }

    public ConsistentHashRing removeNode(Member member) {
        if (!getMembers().contains(member)) {
            return new ConsistentHashRing(buckets, bucketPerNode, replicationFactor);
        }

        List<Bucket> result = Lists.newArrayList(this.buckets);
        int newMemberSize = (buckets.length / bucketPerNode) - 1;

        // remove smallest buckets which is replicated by member
        getBuckets().entrySet().stream()
                .filter(x -> x.getValue().contains(member))
                .sorted((x, y) -> Long.compare(x.getKey().gap(), y.getKey().gap()))
                .limit(bucketPerNode)
                .map(x -> x.getKey().id)
                .sorted((x, y) -> Integer.compare(y, x)) // we need reverse order, otherwise the indexes change
                .forEach(i -> {
                    // cause IntStream doesn't have a method sorted(Comparator)
                    // and you know, it sucks.
                    result.remove((int) i);
                });

        // replace this member to another in other buckets which is replicated by this member
        Stream<Bucket> resultArr = result.stream()
                .map(bucket -> {
                    if(!bucket.members.contains(member))
                        return bucket;
                    HashSet arrayList = new HashSet(bucket.members);
                    arrayList.remove(member);
                    if (newMemberSize >= replicationFactor) {
                        Map<Member, Long> memberTokenRange = new HashMap<>();
                        getBuckets().forEach((val, members) ->
                                members.forEach(m -> memberTokenRange.merge(m, val.gap(), Long::sum)));
                        Optional<Map.Entry<Member, Long>> first = memberTokenRange.entrySet().stream()
                                .filter(x -> !x.getKey().equals(member))
                                .sorted((o1, o2) -> Long.compare(o1.getValue(), o2.getValue()))
                                .findFirst();
                        first.ifPresent(entry -> arrayList.add(entry.getKey()));
                    }
                    return new Bucket(arrayList, bucket.token);
                });

        Bucket[] buckets1 = resultArr.toArray(Bucket[]::new);
        return new ConsistentHashRing(buckets1, bucketPerNode, replicationFactor);
    }

    public int findBucketId(String key) {
        return findBucketIdFromToken(hash(key));
    }

    public Bucket findBucket(String key) {
        return buckets[findBucketIdFromToken(hash(key))];
    }

    public Set<Member> getMembers() {
        HashSet<Member> members = new HashSet<>();

        for (Bucket bucket : buckets) {
            members.addAll(bucket.members);
        }

        return members;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsistentHashRing)) return false;

        ConsistentHashRing that = (ConsistentHashRing) o;

        if (bucketPerNode != that.bucketPerNode) return false;
        if (replicationFactor != that.replicationFactor) return false;
        if (!Arrays.equals(buckets, that.buckets)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = bucketPerNode;
        result = 31 * result + Arrays.hashCode(buckets);
        result = 31 * result + replicationFactor;
        return result;
    }

    public static class Bucket {
        public long token;

        //        @CollectionSerializer.BindCollection(
//                elementSerializer = UnmodifiableCollectionsSerializer.class,
//                elementClass = Member.class,
//                elementsCanBeNull = false)
        public ArrayList<Member> members;

        public Bucket(Set<Member> members, long token) {
            this.members = new ArrayList<>(members);
            this.token = token;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Bucket)) return false;

            Bucket bucket = (Bucket) o;

            if (token != bucket.token) return false;
            if (!members.equals(bucket.members)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (token ^ (token >>> 32));
            result = 31 * result + members.hashCode();
            return result;
        }
    }

    public static class TokenRange {
        public final long start;
        public final long end;
        public final int id;

        private TokenRange(int id, long start, long end) {
            this.id = id;
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return "TokenRange{" +
                    "start=" + start +
                    ", end=" + end +
                    ", bucketId=" + id +
                    '}';
        }

        public long gap() {
            return Math.abs(end - start);
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
