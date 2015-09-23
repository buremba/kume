package org.rakam.kume;

import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 19:06.
 */
public interface MembershipListener {
    default void memberAdded(Member member) {}
    default void memberRemoved(Member member) {}
    default void clusterMerged(Set<Member> newMembers) {}
}
