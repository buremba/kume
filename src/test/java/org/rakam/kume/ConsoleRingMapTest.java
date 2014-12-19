package org.rakam.kume;

import org.junit.Test;
import org.rakam.kume.service.ringmap.RingMap;
import org.rakam.kume.util.ConsistentHashRing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/11/14 19:29.
 */
public class ConsoleRingMapTest {
    @Test
    public void testMap() throws InterruptedException {
        ServiceInitializer services = new ServiceInitializer()
                .add("map", bus -> new RingMap(bus, 2));

        Cluster cluster0 = new ClusterBuilder().services(services).start();
        Cluster cluster1 = new ClusterBuilder().services(services).start();

        RingMap ringMap0 = cluster0.getService("map");
        RingMap ringMap1 = cluster0.getService("map");

        ArrayList<Cluster> instances = new ArrayList<>(2);
        instances.add(cluster0);
        instances.add(cluster1);

        Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(() -> printMapStats(instances), 5, 5, TimeUnit.SECONDS);


        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CountDownLatch countDownLatch = new CountDownLatch(3);

        executorService.execute(() -> {
            for (int i = 0; i < 50000; i++) {
                ringMap0.put("deneme" + i, 4);
            }
            countDownLatch.countDown();
        });
        executorService.execute(() -> {
            for (int i = 0; i < 50000; i++) {
                ringMap1.put("deneme" + i, 4);
            }
            countDownLatch.countDown();
        });


        countDownLatch.await();
    }

    // only for debugging map using console
    public static void printMapStats(Collection<Cluster> list) {
        System.out.printf("| Server            | Range          | Map Size |%n");
        System.out.format("+-------------------+----------------+----------+%n");
        for (Cluster cluster : list) {
            RingMap service = cluster.getService("map");
            double totalRingRange = service.getRing().getTotalRingRange(cluster.getLocalMember());
            System.out.format("| %-17s | %-14f | %-8d |%n", cluster.getLocalMember().getAddress(), totalRingRange, service.getLocalSize());
        }
        System.out.format("+-------------------+----------------+----------+%n");

        RingMap map = list.iterator().next().getService("map");
        Map<ConsistentHashRing.TokenRange, List<Member>> buckets = map.getRing().getBuckets();

        buckets.forEach((token, members) -> {
            double percentage = (Math.abs(token.end-token.start)/2)/(Long.MAX_VALUE/100.0);
            int i1 = ((Double) percentage).intValue()-1;
            System.out.print(token.id);
            for (int i2 = 0; i2 < i1; i2++)
                System.out.print("-");
        });
        System.out.println();
        System.out.println();
    }
}
