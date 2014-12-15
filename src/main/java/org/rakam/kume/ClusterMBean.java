package org.rakam.kume;

import io.netty.channel.Channel;

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
    void sendAllMembersInternal(Object bytes, short service);
    Map<Member, CompletableFuture<Result>> askAllMembersInternal(Object bytes, short service);
    void sendInternal(Channel channel, Object obj, short service);
    CompletableFuture<Result> askInternal(Channel channel, Object obj, short service);
    void close() throws InterruptedException;
}
