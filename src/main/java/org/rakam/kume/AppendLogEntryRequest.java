package org.rakam.kume;

import org.rakam.kume.service.Service;
import org.rakam.kume.transport.OperationContext;
import org.rakam.kume.transport.Request;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
* Created by buremba <Burak Emre Kabakcı> on 03/01/15 19:55.
*/
class AppendLogEntryRequest<R> implements Request<Cluster.InternalService, Boolean> {
    private final Request request;
    private final Function<Service, Boolean> preCheck;
    private final int serviceId;

    public AppendLogEntryRequest(Request request, Function<Service, Boolean> preCheck, int serviceId) {
        this.request = request;
        this.serviceId = serviceId;
        this.preCheck = preCheck;
    }

    @Override
    public void run(Cluster.InternalService service, OperationContext<Boolean> ctx) {
//        synchronized (service.cluster) {
            Set<Member> counter = new HashSet<>();
            Set<Member> members = service.cluster.getMembers();
            AtomicInteger size = new AtomicInteger(members.size());
            long index = service.cluster.getLastCommitIndex().get();
            if(preCheck!=null && !preCheck.apply(service))
                ctx.reply(false);

            ArrayList<CompletableFuture> reqs = new ArrayList(members.size());
            for (Member member : members) {
                CompletableFuture<R> f = new CompletableFuture<>();
                service.cluster.tryAskUntilDoneInternal(member, new UncommittedLogRequest(index, request), 5, serviceId, f);
                reqs.add(f.whenComplete((result, ex) -> {
                    int lastSize;
                    if (ex != null) {
                        service.cluster.removeMemberAsMaster(member, true);
                        lastSize = size.decrementAndGet();
                    } else {
                        counter.add(member);
                        lastSize = counter.size();
                    }

                    if (lastSize == members.size()) {
                        commit(service.cluster, index);
                        ctx.reply(true);
                    }
                }));
            }
            reqs.forEach(f -> f.join());
//        }
    }

    public void commit(Cluster cluster, long index) {
        for (Member member : cluster.getMembers()) {
            CompletableFuture<Object> f0 = new CompletableFuture<>();
            cluster.tryAskUntilDoneInternal(member, new CommitLogRequest(index), 5, serviceId, f0);
            f0.whenComplete((result0, ex0) -> {
                if (ex0 != null && ex0 instanceof TimeoutException) {
                    cluster.removeMemberAsMaster(member, true);
                }
            });
        }
    }

    public static class UncommittedLogRequest implements Request<Cluster.InternalService, Boolean> {
        long index;
        Request request;

        public UncommittedLogRequest(long index, Request request) {
            this.index = index;
            this.request = request;
        }

        @Override
        public void run(Cluster.InternalService service, OperationContext<Boolean> ctx) {
            service.cluster.pendingConsensusMessages().put(index, request);
            ctx.reply(true);
        }
    }
    public static class CommitLogRequest implements Request<Cluster.InternalService, Boolean> {
        long index;

        public CommitLogRequest(long index) {
            this.index = index;
        }

        @Override
        public void run(Cluster.InternalService service, OperationContext<Boolean> ctx) {
            service.cluster.pendingConsensusMessages().get(index).run(service, ctx);
        }
    }
}
