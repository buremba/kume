package org.rakam.kume;

import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 19:06.
 */
public interface MembershipListener {
    void memberAdded(Member member);
    void memberRemoved(Member member);
    void clusterMerged(Set<Member> newMembers);
    void clusterChanged();
}
