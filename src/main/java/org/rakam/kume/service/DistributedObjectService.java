package org.rakam.kume.service;

import org.rakam.kume.Cluster;
import org.rakam.kume.transport.Operation;
import org.rakam.kume.transport.OperationContext;
import org.rakam.kume.util.ConsistentHashRing;
import org.rakam.kume.Member;
import org.rakam.kume.MembershipListener;
import org.rakam.kume.ServiceContext;
import org.rakam.kume.transport.Request;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/12/14 04:25.
 */
public abstract class DistributedObjectService<C extends DistributedObjectService, T> extends Service implements MembershipListener
{
    final int replicationFactor;
    private final ServiceContext<C> ctx;
    private List<Member> ownedMembers;

    static Random random = new Random();

    public List<Member> getOwnedMembers() {
        return Collections.unmodifiableList(ownedMembers);
    }

    // ringStore is used for sharing the ring between services in the same cluster.
    // we could store the ring as instance field but it would take more space since each GCounterService will have its own ring.
    // since there may be multiple clusters that live on same jvm instance, we use a map to store the ring for each cluster.
    private static Map<Cluster, ConsistentHashRing> ringStore = new ConcurrentHashMap<>();

    public DistributedObjectService(ServiceContext clusterContext, int replicationFactor) {
        this.ctx = clusterContext;
        this.replicationFactor = replicationFactor;
        Cluster cluster = ctx.getCluster();
        cluster.addMembershipListener(this);

        ConsistentHashRing ring = ringStore.computeIfAbsent(ctx.getCluster(),
                k -> new ConsistentHashRing(cluster.getMembers(), 1, replicationFactor));
        arrangePartitions(ring);
        ownedMembers = ring.findBucket(ctx.serviceName()).members;
    }

    public ServiceContext<C> getContext() {
        return ctx;
    }

    private void arrangePartitions(ConsistentHashRing ring) {
        List<Member> oldOwnedMembers = ownedMembers;
        ownedMembers = ring.findBucket(ctx.serviceId()).members;
        Member localMember = ctx.getCluster().getLocalMember();

        if (oldOwnedMembers.contains(localMember) && !ownedMembers.contains(localMember)) {
            T counterValue = getLocal();
            setLocal(null);
            ownedMembers.stream()
                    .map(member -> ctx.tryAskUntilDone(member, new MergeRequest<>(counterValue), 5));
        } else if (!oldOwnedMembers.contains(localMember) && ownedMembers.contains(localMember)) {
            // find a way to wait for the counter from other replicas before serving the requests.
            // we may use PausableService but it comes with a big overhead per GCounterService because of the queues in PausableService.
            // TODO: use one request queue for all GCounter in same cluster
        }
    }

    public CompletableFuture<T> syncAndGet() {
        AtomicReference<T> c = new AtomicReference<>();
        CompletableFuture<T>[] map = ownedMembers.stream()
                .map(member -> {
                    CompletableFuture<T> ask = ctx.ask(member, (service, ctx) -> service.getLocal());
                    return ask
                            .thenAccept(x -> c.getAndAccumulate(x, (t, t2) -> {
                                this.mergeIn(t2);
                                return t;
                            }));
                }).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(map).thenApply(x -> c.get());
    }

    public abstract T getLocal();

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

    public abstract void setLocal(T val);

    protected abstract boolean mergeIn(T val);

    private static class MergeRequest<C extends DistributedObjectService, T> implements Request<C, Boolean>
    {
        private final T val;

        public MergeRequest(T val) {
            this.val = val;
        }

        @Override
        public void run(C service, OperationContext<Boolean> ctx) {
            ctx.reply(service.mergeIn(service.getLocal()));
        }
    }

    protected void sendToReplicas(Operation<C> req) {
        ownedMembers.forEach(member -> ctx.send(member, req));
    }

    protected <R> Stream<CompletableFuture<R>> askReplicas(Request<C, R> req) {
        return ownedMembers.stream().map(member -> ctx.ask(member, req));
    }

    protected <R> CompletableFuture<R> askRandomReplica(Request<C, R> req) {
        Member member = ctx.getCluster().getLocalMember();
        if(!ownedMembers.contains(member)) {
            member = ownedMembers.get(random.nextInt(ownedMembers.size()));
        }
        return ctx.ask(member, req);
    }

    protected <R> Stream<CompletableFuture<R>> askReplicas(Request<C, R> req, Class<R> clazz) {
        return askReplicas(req);
    }
}
