package org.rakam.kume;

import io.netty.channel.Channel;
import org.rakam.kume.transport.Operation;
import org.rakam.kume.transport.OperationContext;
import org.rakam.kume.transport.Request;
import org.rakam.kume.util.FutureUtil;
import org.rakam.kume.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by buremba <Burak Emre Kabakcı> on 28/12/14 20:07.
 */
public class ClusterCheckAndMergeOperation implements Operation<InternalService> {
    final static Logger LOGGER = LoggerFactory.getLogger(ClusterCheckAndMergeOperation.class);

    @Override
    public void run(InternalService service, OperationContext<Void> ctx) {
        Cluster cluster = service.cluster;

        Set<Member> clusterMembers = cluster.getMembers();
        Member masterNode = cluster.getMaster();
        checkState(masterNode.equals(cluster.getLocalMember()), "only master node must execute ClusterCheckAndMergeOperation.");

        Member sender = ctx.getSender();
        if (clusterMembers.contains(sender))
            return;

        LOGGER.trace("got cluster check and merge request from a server who is not in this cluster");

        Channel channel;
        try {
            channel = cluster.connectServer(ctx.getSender().address);
        } catch (InterruptedException e) {
            LOGGER.trace("a server send me join request from udp but i can't connect him. ignoring");
            return;
        }

        LOGGER.trace("connected to the new node, now getting cluster information");

        cluster.clusterConnection.put(ctx.getSender(), channel);

        final Tuple<Set<Member>, Long> clusterStatus;
        try {
            clusterStatus = cluster.internalBus
                    .tryAskUntilDone(ctx.getSender(), new GetInformationFromDiscoveredCluster(), 5, Tuple.class)
                    .join();
            LOGGER.trace("got answer from discovered cluster: " + clusterStatus._1.size()
                    + "members, last commit index: "+clusterStatus._2);
        } catch (CompletionException e) {
            cluster.clusterConnection.remove(ctx.getSender());
            return;
        }
        Set<Member> otherMembers = new HashSet<>(clusterStatus._1);
        Set<Member> members = cluster.getMembers();
        otherMembers.removeAll(members);

        synchronized (cluster) {
            int otherSize = otherMembers.size();
            int mySize = cluster.getMembers().size();

            long myCommitIndex = cluster.getLastCommitIndex().get();
            Long otherCommitIndex = clusterStatus._2;

            if (otherSize > mySize || (otherSize == mySize && myCommitIndex < otherCommitIndex)) {
                LOGGER.trace("they will eventually add me");
                return;
            }

            synchronized (cluster) {
                LOGGER.trace("they must join me, my cluster is bigger than theirs");
                for (Member otherMember : otherMembers) {
                    Channel memberChannel;
                    try {
                        memberChannel = cluster.connectServer(otherMember.address);
                    } catch (InterruptedException e) {
                        otherMembers.remove(otherMember);
                        continue;
                    }
                    cluster.clusterConnection.put(otherMember, memberChannel);
                }

                FutureUtil.MultipleFutureListener f = new FutureUtil.MultipleFutureListener(otherMembers.size());
                LOGGER.trace("asking new members to join our party.");
                for (Member otherMember : otherMembers) {
                    CompletableFuture<Void> ask = cluster.internalBus
                            .tryAskUntilDone(otherMember, new JoinThisClusterRequest(cluster.getMembers(), masterNode), 5);
                    ask.whenComplete((result, ex) -> {
                        if (ex != null) {
                            otherMembers.remove(otherMember);
                            cluster.clusterConnection.remove(otherMember);
                            LOGGER.trace(otherMember + " was a member of the new cluster but since I (master) couldn't connect it," +
                                    " it will be ignored.");
                        } else {
                            LOGGER.trace(otherMember + " successfully connected to master");
                        }
                        f.increment();
                    });
                }

                LOGGER.trace("waiting responses from new members");
                f.get().join();
                LOGGER.trace(otherMembers.size() + " members is added to the cluster");

                LOGGER.trace("replicating information about new members to cluster members");
                cluster.internalBus.replicateSafely(new MembersJoinedRequest(otherMembers)).join();
                LOGGER.trace("all members in other cluster successfully added into this cluster");
            }
        }
    }

    public static class JoinThisClusterRequest implements Request<InternalService, Void> {
        final Set<Member> members;
        final Member masterNode;

        public JoinThisClusterRequest(Set<Member> members, Member masterNode) {
            this.members = members;
            this.masterNode = masterNode;
        }

        @Override
        public void run(InternalService service, OperationContext<Void> ctx) {
            LOGGER.trace("someone wants me in his cluster, i will join her party.");
            service.cluster.changeCluster(members, masterNode, false);
            ctx.reply(null);
        }
    }

    public static class MembersJoinedRequest implements Request<InternalService, Boolean> {
        final Set<Member> members;

        public MembersJoinedRequest(Set<Member> members) {
            this.members = members;
        }

        @Override
        public void run(InternalService service, OperationContext<Boolean> ctx) {
            LOGGER.trace("there are new members in the cluster. welcoming them.");
            if(members.size() > 1)
                service.cluster.addMembersInternal(members);
            else
            if(members.size() == 1)
                service.cluster.addMemberInternal(members.iterator().next());

            ctx.reply(null);
        }
    }

    public static class GetInformationFromDiscoveredCluster implements Request<InternalService, Tuple> {

        @Override
        public void run(InternalService service, OperationContext<Tuple> ctx) {
            Cluster cluster1 = service.cluster;
            Tuple<Set, Long> obj = new Tuple<>(cluster1.getMembers(), cluster1.getLastCommitIndex().get());
            LOGGER.trace("i was asked my cluster status. here it is: " +
                    obj._1.size() + "members, last commit index: " + obj._2);
            ctx.reply(obj);
        }
    }

}
