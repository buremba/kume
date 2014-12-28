package org.rakam.kume;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/11/14 18:51.
 */
public class KumeTest {

    @Test
    public void waitForDiscovery() {
        CompletableFuture<Void> f = new CompletableFuture<>();

        CompletableFuture<Void> c = f.thenAccept(x -> {
            System.out.println(1);
        });
        CompletableFuture<Void> d = c.thenAccept(x -> {
            System.out.println(2);
        });

        c.complete(null);
    }

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

            @Override
            public void clusterMerged() {

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

            @Override
            public void clusterMerged() {

            }
        });
        countDownLatch.await();
    }

    @Test
    public void test() {
        CompletableFuture<Boolean> booleanCompletableFuture = CompletableFuture.completedFuture(true);
        CompletableFuture.allOf(booleanCompletableFuture).thenRun(() -> {
            System.out.println("a");
        });
    }
}
