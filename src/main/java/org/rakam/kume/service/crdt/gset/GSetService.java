package org.rakam.kume.service.crdt.gset;

import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.MembershipListener;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;
import org.rakam.kume.service.Service;
import org.rakam.kume.util.ConsistentHashRing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by buremba <Burak Emre Kabakcı> on 28/12/14 21:26.
 */
public class GSetService<T> implements Service, MembershipListener {
    final int replicationFactor;
    private final Cluster.ServiceContext<GSetService> ctx;
    private List<Member> ownedMembers;
    // ringStore is used for sharing the ring between services in the same cluster.
    // we could store the ring as instance field but it would take more space since each GSetService will have its own ring.
    // since there may be multiple cluster that lives on same jvm instance, we use a map to store the ring for each cluster.
    private static Map<Cluster, ConsistentHashRing> ringStore = new ConcurrentHashMap<>();
    GSet<T> set;

    public GSetService(Cluster.ServiceContext<GSetService> clusterContext, int replicationFactor) {
        this.replicationFactor = replicationFactor;
        ctx = clusterContext;
        Cluster cluster = ctx.getCluster();
        cluster.addMembershipListener(this);

        ConsistentHashRing ring = ringStore.computeIfAbsent(ctx.getCluster(),
                k -> new ConsistentHashRing(cluster.getMembers(), 1, replicationFactor));
        arrangePartitions(ring);
        set = new GSet();
        ownedMembers = ring.findBucket(ctx.serviceName()).members;
    }

    private synchronized void arrangePartitions(ConsistentHashRing ring) {
        List<Member> oldOwnedMembers = ownedMembers;
        ownedMembers = ring.findBucket(ctx.serviceName()).members;
        Member localMember = ctx.getCluster().getLocalMember();

        if (oldOwnedMembers.contains(localMember) && !ownedMembers.contains(localMember)) {
            GSet setValue = set;
            set = null;
            Stream<CompletableFuture<GSet>> stream = ownedMembers.stream()
                    .map(member -> ctx.tryAskUntilDone(member, new MergeRequest(setValue), 5));
        } else if (!oldOwnedMembers.contains(localMember) && ownedMembers.contains(localMember)) {
            // find a way to wait for the cluster from other replicas before serving the requests.
            // we may use PausableService but it comes with a big overhead per GSetService because of the queues in PausableService.
            // TODO: use reentrant lock (overhead?) or use one request queue for all GSet in same cluster
        }
    }

    public void add(long l) {
        checkArgument(l > 0, "value (%s) must be a positive integer", l);
        ownedMembers.forEach(member -> ctx.send(member, (service, ctx) -> service.set.add(l)));
    }


    public CompletableFuture<GSet> syncAndGet() {
        AtomicReference<GSet> c = new AtomicReference<>();
        CompletableFuture<GSet>[] map = ownedMembers.stream()
                .map(member -> ctx.ask(member, (service, ctx) -> service.getLocal(), GSet.class)
                        .thenAccept(x -> c.getAndAccumulate(x, (a, b) -> GSet.merge(a,b)))).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(map).thenApply(x -> c.get());
    }

    public CompletableFuture<Integer> asyncAndSize() {
        AtomicReference<Integer> c = new AtomicReference<>();
        CompletableFuture<GSet>[] map = ownedMembers.stream()
                .map(member -> ctx.ask(member, (service, ctx) -> service.sizeLocal(), Integer.class)
                        .thenAccept(x -> c.getAndAccumulate(x, Math::max))).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(map).thenApply(x -> c.get());
    }

    public GSet<T> getLocal() {
        return set;
    }

    public int sizeLocal() {
        return set.size();
    }

    @Override
    public void memberAdded(Member member) {
        ConsistentHashRing ring = ringStore.get(ctx.getCluster());

        if(!ring.getMembers().contains(member)) {
            ConsistentHashRing newRing = ring.addNode(member);
            ringStore.put(ctx.getCluster(), newRing);
            arrangePartitions(newRing);
        }else {
            // ring is already modified by another GSetService
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
            // ring is already modified by another GSetService
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
            // ring is already modified by another GSetService
            arrangePartitions(ring);
        }
    }

    @Override
    public void clusterChanged() {

    }

    @Override
    public void onClose() {

    }

    private static class MergeRequest implements Request<GSetService, GSet> {
        private final GSet setValue;

        public MergeRequest(GSet setValue) {
            this.setValue = setValue;
        }

        @Override
        public void run(GSetService service, OperationContext<GSet> ctx) {
            GSet serviceVal = service.getLocal();
            GSet newVal = GSet.merge(setValue, serviceVal);
            if (newVal.size() > serviceVal.size()) {
                serviceVal.addAll(newVal);
                ctx.reply(serviceVal);
            } else {
                newVal.addAll(serviceVal);
                ctx.reply(newVal);
            }
        }
    }

    private void set(GSet newVal) {
        set = newVal;
    }

    protected void addAllLocal(Collection<T> items) {
        set.addAll(items);
    }
}
