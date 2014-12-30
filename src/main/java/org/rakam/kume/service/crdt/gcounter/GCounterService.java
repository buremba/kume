package org.rakam.kume.service.crdt.gcounter;

import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.MembershipListener;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;
import org.rakam.kume.service.Service;
import org.rakam.kume.util.ConsistentHashRing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/12/14 04:25.
 */
public class GCounterService implements MembershipListener, Service {
    final int replicationFactor;
    private final Cluster.ServiceContext<GCounterService> ctx;
    private List<Member> ownedMembers;
    // ringStore is used for sharing the ring between services in the same cluster.
    // we could store the ring as instance field but it would take more space since each GCounterService will have its own ring.
    // since there may be multiple cluster that lives on same jvm instance, we use a map to store the ring for each cluster.
    private static Map<Cluster, ConsistentHashRing> ringStore = new ConcurrentHashMap<>();
    GCounter counter;

    public GCounterService(Cluster.ServiceContext<GCounterService> clusterContext, GCounter counter, int replicationFactor) {
        this.replicationFactor = replicationFactor;
        ctx = clusterContext;
        Cluster cluster = ctx.getCluster();
        cluster.addMembershipListener(this);

        ConsistentHashRing ring = ringStore.computeIfAbsent(ctx.getCluster(),
                k -> new ConsistentHashRing(cluster.getMembers(), 1, replicationFactor));
        arrangePartitions(ring);
        this.counter = counter;
        ownedMembers = ring.findBucket(ctx.serviceName()).members;
    }

    public GCounterService(Cluster.ServiceContext<GCounterService> clusterContext, int replicationFactor) {
        this(clusterContext, new GCounter(), replicationFactor);
    }

    private synchronized void arrangePartitions(ConsistentHashRing ring) {
        List<Member> oldOwnedMembers = ownedMembers;
        ownedMembers = ring.findBucket(ctx.serviceId()).members;
        Member localMember = ctx.getCluster().getLocalMember();

        if (oldOwnedMembers.contains(localMember) && !ownedMembers.contains(localMember)) {
            long counterValue = counter.get();
            counter = null;
            Stream<CompletableFuture<Long>> stream = ownedMembers.stream()
                    .map(member -> ctx.tryAskUntilDone(member, new MergeRequest(counterValue), 5));
        } else if (!oldOwnedMembers.contains(localMember) && ownedMembers.contains(localMember)) {
            // find a way to wait for the counter from other replicas before serving the requests.
            // we may use PausableService but it comes with a big overhead per GCounterService because of the queues in PausableService.
            // TODO: use reentrant lock (overhead?) or use one request queue for all GCounter in same cluster
        }
    }

    public void increment() {
        ownedMembers.forEach(member -> ctx.send(member, (service, ctx) -> service.counter.increment()));
    }

    public void add(long l) {
        checkArgument(l > 0, "value (%s) must be a positive integer", l);
        ownedMembers.forEach(member -> ctx.send(member, (service, ctx) -> service.counter.add(l)));
    }


    public CompletableFuture<Long> syncAndGet() {
        AtomicReference<Long> c = new AtomicReference<>();
        CompletableFuture<GCounter>[] map = ownedMembers.stream()
                .map(member -> ctx.ask(member, (service, ctx) -> service.get(), Long.class)
                        .thenAccept(x -> c.getAndAccumulate(x, GCounter::combine))).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(map).thenApply(x -> c.get());
    }

    public long get() {
        return counter.get();
    }

    @Override
    public void memberAdded(Member member) {
        ConsistentHashRing ring = ringStore.get(ctx.getCluster());

        if(!ring.getMembers().contains(member)) {
            ConsistentHashRing newRing = ring.addNode(member);
            ringStore.put(ctx.getCluster(), newRing);
            arrangePartitions(newRing);
        }else {
            // ring is already modified by another GCounterService
            arrangePartitions(ring);
        }
    }

    @Override
    public void memberRemoved(Member member) {
        ConsistentHashRing ring = ringStore.get(ctx.getCluster());

        if(ring.getMembers().contains(member)) {
            ConsistentHashRing newRing = ring.removeNode(member);
            ringStore.put(ctx.getCluster(), newRing);
            arrangePartitions(newRing);
        }else {
            // ring is already modified by another GCounterService
            arrangePartitions(ring);
        }
    }

    @Override
    public void clusterMerged(Set<Member> newMembers) {
        ConsistentHashRing ring = ringStore.get(ctx.getCluster());

        Set<Member> members = ring.getMembers();

        if(!newMembers.containsAll(members)) {
            for (Member newMember : newMembers) {
                ring = ring.addNode(newMember);
            }
            ringStore.put(ctx.getCluster(), ring);
            arrangePartitions(ring);
        }else {
            // ring is already modified by another GCounterService
            arrangePartitions(ring);
        }
    }

    @Override
    public void clusterChanged() {

    }

    private synchronized void setCounter(long l) {
        counter = new GCounter(l);
    }

    @Override
    public void onClose() {

    }

    private static class MergeRequest implements Request<GCounterService, Long> {
        private final long counterValue;

        public MergeRequest(long counterValue) {
            this.counterValue = counterValue;
        }

        @Override
        public void run(GCounterService service, OperationContext<Long> ctx) {
            long serviceVal = service.counter.get();
            long newVal = GCounter.combine(counterValue, serviceVal);
            if (newVal != serviceVal)
                service.setCounter(newVal);
            ctx.reply(newVal);

        }
    }
}
