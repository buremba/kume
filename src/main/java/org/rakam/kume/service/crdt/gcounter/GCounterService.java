package org.rakam.kume.service.crdt.gcounter;

import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.MembershipListener;
import org.rakam.kume.service.Service;
import org.rakam.kume.util.ConsistentHashRing;

import java.util.ArrayList;
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
    private List<Member> ownedMembers;
    GCounter counter;

    public GCounterService(Cluster.ServiceContext<GCounterService> clusterContext, int replicationFactor) {
        this.replicationFactor = replicationFactor;
        ctx = clusterContext;
        Cluster cluster = ctx.getCluster();
        cluster.addMembershipListener(this);
        ConsistentHashRing ring = new ConsistentHashRing(cluster.getMembers(), 1, replicationFactor);
        ownedMembers = ring.findBucket(ctx.serviceName()).members;
        if(ownedMembers.contains(cluster.getLocalMember())) {
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


    public CompletableFuture<GCounter> get() {
        AtomicReference<GCounter> c = new AtomicReference<>();
        CompletableFuture[] map = ownedMembers.stream()
                .map(member -> ctx.ask(member, (service, ctx) -> service.get()).thenAccept(x -> {
            if (x.isSucceeded()) {
                c.getAndAccumulate((GCounter) x.getData(), GCounter::combine);
            }
        })).toArray(CompletableFuture[]::new);


        return CompletableFuture.allOf(map).thenApply(x -> c.get());
    }

    @Override
    public void memberAdded(Member member) {

    }

    @Override
    public void memberRemoved(Member member) {

    }

    private void memberChanged() {
        Cluster cluster = ctx.getCluster();
        ConsistentHashRing ring = new ConsistentHashRing(cluster.getMembers(), 1, replicationFactor);
        ArrayList<Member> newOwnedMembers = ring.findBucket(ctx.serviceName()).members;
        if(ownedMembers.contains(cluster.getLocalMember())) {
            counter = new GCounter();
        }
    }
}
