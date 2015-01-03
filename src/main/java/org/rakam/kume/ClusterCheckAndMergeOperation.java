package org.rakam.kume;

import io.netty.channel.Channel;
import org.rakam.kume.transport.Operation;
import org.rakam.kume.transport.OperationContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkState;

/**
* Created by buremba <Burak Emre Kabakcı> on 28/12/14 20:07.
*/
public class ClusterCheckAndMergeOperation implements Operation<Cluster.InternalService> {

    private final int clusterSize;
    private final long timeRunning;

    public ClusterCheckAndMergeOperation(int clusterSize, long timeRunning) {
        this.clusterSize = clusterSize;
        this.timeRunning = timeRunning;
    }

    @Override
    public void run(Cluster.InternalService service, OperationContext<Void> ctx) {
        Cluster cluster = service.cluster;

        Set<Member> clusterMembers = cluster.getMembers();
        Member masterNode = cluster.getMaster();
        checkState(masterNode.equals(cluster.getLocalMember()), "only master node must execute ClusterCheckAndMergeOperation.");

        Member sender = ctx.getSender();
        if (clusterMembers.contains(sender))
            return;

        if (clusterMembers.size() > clusterSize)
            return;

        long myTimeRunning = System.currentTimeMillis() - cluster.startTime();
        if (clusterMembers.size() == clusterSize && myTimeRunning < timeRunning)
            return;

        Channel channel;
        try {
            channel = cluster.connectServer(ctx.getSender().address);
        } catch (InterruptedException e) {
            return;
        }

        synchronized (cluster) {
            cluster.clusterConnection.put(ctx.getSender(), channel);
            cluster.addMemberInternal(ctx.getSender());

            final Set<Member> otherClusterMembers;
            try {
                otherClusterMembers = cluster.internalBus
                        .tryAskUntilDone(ctx.getSender(), (service0, ctx0) -> {
                            service0.cluster.changeCluster(clusterMembers, masterNode, false);
                            ctx0.reply(service0.cluster.getMembers());
                        }, 5, Set.class)
                        .join();
            } catch (CompletionException e) {
                cluster.removeMemberAsMaster(ctx.getSender(), false);
                return;
            }
            Set<Member> otherMembers = new HashSet<>(otherClusterMembers);

            ArrayList<CompletableFuture> mergeClusterRequest = new ArrayList(otherMembers.size());
            Iterator<Member> iterator = otherMembers.iterator();
            while(iterator.hasNext()) {
                Member otherClusterMember = iterator.next();
                if (otherClusterMember.equals(ctx.getSender()))
                    continue;
                if (clusterMembers.contains(otherClusterMember)) {
                    iterator.remove();
                    continue;
                }

                Channel memberChannel;
                try {
                    memberChannel = cluster.connectServer(otherClusterMember.address);
                } catch (InterruptedException e) {
                    otherMembers.remove(ctx.getSender());
                    continue;
                }
                cluster.clusterConnection.put(otherClusterMember, memberChannel);
                cluster.addMemberInternal(ctx.getSender());

                CompletableFuture<Object> f = cluster.internalBus
                        .tryAskUntilDone(otherClusterMember, (service0, ctx0) -> {
                            service0.cluster.changeCluster(clusterMembers, masterNode, false);
                            ctx0.reply(true);
                        }, 5).whenComplete((val, ex) -> cluster.removeMemberAsMaster(otherClusterMember, true));
                mergeClusterRequest.add(f);
            }

            cluster.getMembers().stream()
                    .map(member -> cluster.internalBus
                            .tryAskUntilDone(member, (service0, ctx2) -> service.cluster.addMembersInternal(otherMembers), 5)
                            .whenComplete((result, ex) -> {
                                if (ex != null && ex instanceof TimeoutException)
                                    cluster.removeMemberAsMaster(member, true);
                            })).forEach(CompletableFuture::join);
            mergeClusterRequest.forEach(r -> r.join());

        }
    }

}
