package org.rakam.kume;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 19:06.
 */
public interface MembershipListener {
    public void memberAdded(Member member);
    public void memberRemoved(Member member);
}
