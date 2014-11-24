package org.rakam.kume.util;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.rakam.kume.Member;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/11/14 17:17.
 * This class is immutable.
 */
public class ConsistentHashRing {
    private final int bucketPerNode;
    final Node[] buckets;
    private final HashFunction hashFunction = Hashing.murmur3_128();

    public ConsistentHashRing(Collection<Member> members, int bucketPerNode) {
        this.bucketPerNode = bucketPerNode;

        // todo: find a way to construct buckets without adding elements one by one.
        Iterator<Member> iterator = members.iterator();
        if(iterator.hasNext()) {
            Node[] list = findBucketListForNewNode(iterator.next(), bucketPerNode, null);
            while(iterator.hasNext()) {
                list = findBucketListForNewNode(iterator.next(), bucketPerNode, list);
            }
            buckets = list;
        }else {
            throw new IllegalArgumentException("member list must have at least one member");
        }
    }

    public List<Node> getBuckets() {
        return Collections.unmodifiableList(Arrays.asList(buckets));
    }

    private ConsistentHashRing(Node[] buckets, int bucketPerNode) {
        this.bucketPerNode = bucketPerNode;
        this.buckets = buckets;
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

    public Set<Member> findOwnedNodes(String hash) {
        long l = hashFunction.hashString(hash, Charset.forName("UTF-8")).asLong();
        return findNextBackups(findClosest(l), 2);
    }

    public Set<Member> findNextBackups(int bucketId, int numberOfNodes) {
        HashSet<Member> objects = new HashSet<>(numberOfNodes);
        int id = bucketId;
        int addrCount = Math.min(numberOfNodes, buckets.length / bucketPerNode);

        while (objects.size() < addrCount) {
            objects.add(buckets[id].member);
            id = (id + 1) % buckets.length;
        }

        return objects;
    }

    public Collection<Member> findNode(String hash) {
        long l = hashFunction.hashString(hash, Charset.forName("UTF-8")).asLong();
        return findNextBackups(findClosest(l), 2);
    }

    public boolean isSibling(Member member1, Member member2) {
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i].member.equals(member1)) {
                Node previous = buckets[ringPos(i-1)];
                HashSet<Member> nodes = new HashSet<>();
                nodes.add(previous.member);
                int a = i;
                while (nodes.size() < 2) {
                    nodes.add(buckets[a].member);
                    a = (a + 1) % buckets.length;
                }
                if (nodes.contains(member2)) {
                    return true;
                }
            }
        }
        return false;
    }

    private int ringPos(int i) {
        int length = buckets.length;
        return (i % length) + (i < 0 ? length : 0);
    }

    public double getTotalRingRange(Member member) {
        double total = 0;
        for (int i = 0; i < buckets.length; i++) {
            Node current = buckets[i];
            if(current.member.equals(member)) {
                if(i == buckets.length-1) {
                    total += Math.abs((Long.MAX_VALUE-current.token)/2)/(Long.MAX_VALUE/100.0);
                }else {
                    total += Math.abs((buckets[i+1].token-current.token)/2)/(Long.MAX_VALUE/100.0);
                }

            }
        }
        return total;
    }

    private static Node[] findBucketListForNewNode(Member member, int bucketPerNode, Node[] buckets) {
        if (buckets == null) {
            Node[] newNodeList = new Node[bucketPerNode];

            long token = Long.MAX_VALUE / (bucketPerNode/2);
            for (int i = 0; i < bucketPerNode; i++) {
                long t = Long.MIN_VALUE + (token * i);
                newNodeList[i] = new Node(member, t);
            }
            return newNodeList;
        }

        Node[] newNodeList = Arrays.copyOf(buckets, buckets.length + bucketPerNode);

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
            Node element = new Node(member, current.token + (Long) current.gap / 2);
            newNodeList[idx + buckets.length] = element;
        }
        Arrays.sort(newNodeList, (o1, o2) -> o2.token > o1.token ? -1 : (o2.token == o1.token ? 0 : 1));

        return newNodeList;
    }

    public ConsistentHashRing addNode(Member member) {
        Node[] buckets = findBucketListForNewNode(member, bucketPerNode, this.buckets);
        return new ConsistentHashRing(buckets, bucketPerNode);
    }

    public ConsistentHashRing removeNode(Member member) {
        List<Node> result = Lists.newArrayList(buckets);

        for (int i = 0; i < result.size(); i++) {
            if (result.get(i).member.equals(member));
            result.remove(i);
        }

        Node[] nodes = new Node[result.size()];
        result.toArray(nodes);
        return new ConsistentHashRing(nodes, bucketPerNode);
    }

    public static class Node {
        public final long token;
        public final Member member;

        public Node(Member member, long token) {
            this.member = member;
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
}
