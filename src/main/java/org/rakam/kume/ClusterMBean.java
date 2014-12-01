package org.rakam.kume;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 28/11/14 16:41.
 */
public interface ClusterMBean {
    void addMember(Member member);
    void removeMember(Member member);
    void addMembershipListener(MembershipListener listener);
    java.util.Set<Member> getMembers();
    Map<Member, ChannelFuture> sendAllMembersInternal(Object bytes, short service);
    Map<Member, CompletableFuture<Result>> askAllMembersInternal(Object bytes, short service);
    ChannelFuture sendInternal(Channel channel, Object obj, short service);
    CompletableFuture<Result> askInternal(Channel channel, Object obj, short service);
    void close() throws InterruptedException;
}
