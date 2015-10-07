/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.kume.join.multicast;

import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterCheckAndMergeOperation;
import org.rakam.kume.ClusterMembership;
import org.rakam.kume.JoinerService;
import org.rakam.kume.Member;
import org.rakam.kume.ServiceContext;
import org.rakam.kume.transport.Request;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import org.rakam.kume.MemberState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class MulticastJoinerService implements JoinerService
{
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    private final MulticastServerHandler multicastServer;
    private final Cluster cluster;
    private final Member localMember;
    NioEventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private AtomicInteger currentTerm;
    private long lastContactedTimeMaster;
    private Member master;
    final private Map<Member, Long> heartbeatMap = new ConcurrentHashMap<>();

    private ScheduledFuture<?> heartbeatTask;
    private ConcurrentMap<InetSocketAddress, Integer> pendingUserVotes = CacheBuilder.newBuilder().expireAfterWrite(100, TimeUnit.SECONDS).<InetSocketAddress, Integer>build().asMap();
    private MemberState memberState;

    private Map<Long, Request> pendingConsensusMessages = new ConcurrentHashMap<>();
    private AtomicLong lastCommitIndex = new AtomicLong();

    public MulticastJoinerService(ServiceContext ctx) {
        cluster = ctx.getCluster();
        localMember = cluster.getLocalMember();

        InetSocketAddress multicastAddress = new InetSocketAddress("224.0.67.67", 5001);
        try {
            multicastServer = new MulticastServerHandler(ctx.getCluster(), multicastAddress)
                    .start();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to bind UDP " + multicastAddress);
        }
        LOGGER.info("{} started , listening UDP multicast server {}", localMember, multicastAddress);
        multicastServer.setAutoRead(true);
    }

    private synchronized void changeMaster(Member masterMember) {
        master = masterMember;
        memberState = masterMember.equals(localMember) ? MemberState.MASTER : MemberState.FOLLOWER;
        multicastServer.setJoinGroup(memberState == MemberState.MASTER);
    }

    public MemberState memberState() {
        return memberState;
    }

    public synchronized void removeMemberAsMaster(Member member, boolean replicate) {
//        if (!isMaster())
//            throw new IllegalStateException();

//        heartbeatMap.remove(member);
//        members.remove(member);
//        if(replicate) {

//        internalBus.sendAllMembers((cluster, ctx) -> {
//            cluster.clusterConnection.remove(member);
//            Cluster.LOGGER.info("Member removed {}", member);
//            cluster.membershipListeners.forEach(l -> Throwables.propagate(() -> l.memberRemoved(member)));
//        }, true);
//        }
    }



//        workerGroup.scheduleWithFixedDelay(() -> {
//            ClusterCheckAndMergeOperation req = new ClusterCheckAndMergeOperation();
//            multicastServer.sendMulticast(req);
//        }, 0, 2000, TimeUnit.MILLISECONDS);



    public void joinCluster() {
        multicastServer.sendMulticast(new ClusterCheckAndMergeOperation());
    }

    protected synchronized void changeCluster(Set<Member> newClusterMembers, Member masterMember, boolean isNew) {
//        try {
//            pause();
//            clusterConnection.clear();
//            master = masterMember;
//            members = newClusterMembers;
//            messageHandlers.cleanUp();
//            LOGGER.info("Joined a cluster of {} nodes.", members.size());
//            multicastServer.setJoinGroup(masterMember.equals(localMember));
//            if (!isNew)
//                membershipListeners.forEach(x -> eventLoop.execute(() -> x.clusterChanged()));
//        } finally {
//            resume();
//        }
    }

    @Override
    public void onClose() {
        heartbeatTask.cancel(true);

        try {
            multicastServer.close();
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void onStart(ClusterMembership membership) {

    }
}
