package org.rakam.kume;

import java.util.concurrent.CountDownLatch;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/11/14 18:51.
 */
public class KumeTest {

    public static void waitForDiscovery(Cluster cluster, int numberOfInstances) throws InterruptedException {
        int i = numberOfInstances - cluster.getMembers().size();
        if(i <= 0)
            return;

        CountDownLatch countDownLatch = new CountDownLatch(i);

        cluster.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(Member member) {
                countDownLatch.countDown();
            }

            @Override
            public void memberRemoved(Member member) {

            }
        });
        countDownLatch.await();
    }

    public static void waitForNodeToLeave(Cluster cluster, int numberOfInstances) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(numberOfInstances);

        cluster.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(Member member) {

            }

            @Override
            public void memberRemoved(Member member) {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
    }
}
