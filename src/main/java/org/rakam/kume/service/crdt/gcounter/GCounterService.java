package org.rakam.kume.service.crdt.gcounter;

import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.MembershipListener;
import org.rakam.kume.service.Service;
import org.rakam.kume.util.ConsistentHashRing;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/12/14 04:25.
 */
public class GCounterService implements Service, MembershipListener {
    final int replicationFactor;
    private final Cluster.ServiceContext<GCounterService> ctx;
    private final ConsistentHashRing ring;
    private List<Member> ownedMembers;
    GCounter counter;

    public GCounterService(Cluster.ServiceContext<GCounterService> clusterContext, int replicationFactor) {
        this.replicationFactor = replicationFactor;
        ctx = clusterContext;
        Cluster cluster = ctx.getCluster();
        cluster.addMembershipListener(this);
        ring = new ConsistentHashRing(cluster.getMembers(), 1, replicationFactor);
        arrangePartitions(ring);
    }

    private synchronized void arrangePartitions(ConsistentHashRing ring) {
        ownedMembers = ring.findBucket(ctx.serviceName()).members;
        if(ownedMembers.contains(ctx.getCluster().getLocalMember())) {
            counter = new GCounter();
        }
    }

    public void increment() {
        ownedMembers.forEach(member -> ctx.send(member, (service, ctx) -> service.counter.increment()));
    }

    public void add(long l) {
        checkArgument(l > 0, "value (%s) must be a positive integer", l);
        ownedMembers.forEach(member -> ctx.send(member, (service, ctx) -> service.counter.add(l)));
    }


    public CompletableFuture<GCounter> asyncAndGet() {
        AtomicReference<GCounter> c = new AtomicReference<>();
        CompletableFuture<GCounter>[] map = ownedMembers.stream()
                .map(member -> {
                    CompletableFuture<GCounter> ask = ctx.ask(member, (service, ctx) -> service.get());
                    ask.thenAccept(x -> c.getAndAccumulate(x, GCounter::combine));
                    return ask;
                }).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(map).thenApply(x -> c.get());
    }

    public long get() {
        return counter.get();
    }

    @Override
    public void memberAdded(Member member) {
        arrangePartitions(ring.addNode(member));
    }

    @Override
    public void memberRemoved(Member member) {
        arrangePartitions(ring.removeNode(member));
    }

    @Override
    public void clusterChanged() {
        ConsistentHashRing ring = new ConsistentHashRing(ctx.getCluster().getMembers(), 1, replicationFactor);
        arrangePartitions(ring);
    }
}
