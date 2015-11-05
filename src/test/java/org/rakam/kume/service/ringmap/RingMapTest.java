package org.rakam.kume.service.ringmap;

import com.google.common.collect.ImmutableList;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.kume.ClusterMembership;
import org.rakam.kume.JoinerService;
import org.rakam.kume.KumeTest;
import org.rakam.kume.Member;
import org.rakam.kume.MigrationListener;
import org.rakam.kume.NoNetworkTransport;
import org.rakam.kume.service.ServiceListBuilder;
import org.rakam.kume.service.crdt.counter.GCounterService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class RingMapTest extends KumeTest {

//    @Test
    public void testMapNotEnoughNodeForReplication() throws InterruptedException, TimeoutException, ExecutionException, IOException {

        ImmutableList<ServiceListBuilder.Constructor> services = new ServiceListBuilder()
                .add("map", bus -> new RingMap<String, Long>(bus, GCounterService::merge, 2)).build();

        Cluster cluster0 = new ClusterBuilder().services(services).start();

        RingMap ringMap0 = cluster0.getService("map");

        for (int i = 0; i < 1000; i++) {
            ringMap0.put("test" + System.currentTimeMillis() + i, i).get();
        }

        assertEquals(ringMap0.getLocalSize(), 1000);
    }


//    @Test
    public void testMapReplication() throws InterruptedException, TimeoutException, ExecutionException {
        ImmutableList<ServiceListBuilder.Constructor> services = new ServiceListBuilder()
                .add("map", bus -> new RingMap<String, Long>(bus, GCounterService::merge, 2)).build();

        List<Cluster> clusters = createFixedFakeCluster(2, services).map(ClusterBuilder::start).collect(Collectors.toList());;

        RingMap ringMap0 = clusters.get(0).getService("map");
        RingMap ringMap1 = clusters.get(1).getService("map");

        for (int i = 0; i < 100000; i++) {
            ringMap0.put("test" + i, 5).get();
        }

        assertEquals(ringMap1.getLocalSize(), ringMap1.getLocalSize());

        for (int i = 0; i < ringMap0.getBucketCount(); i++) {
            assertEquals(ringMap0.getBucket(i), ringMap1.getBucket(i));
        }
    }

//    @Test
    public void testMapDistribution() throws InterruptedException, TimeoutException, ExecutionException {
        ImmutableList<ServiceListBuilder.Constructor> services = new ServiceListBuilder()
                .add("map", bus -> new RingMap<String, Long>(bus, GCounterService::merge, 1)).build();

        List<Cluster> clusters = createFixedFakeCluster(2, services).map(ClusterBuilder::start).collect(Collectors.toList());

        RingMap ringMap0 = clusters.get(0).getService("map");
        RingMap ringMap1 = clusters.get(1).getService("map");

        for (int i = 0; i < 100000; i++) {
            ringMap0.put("test" + i, 5).get();
        }

        assertEquals(ringMap1.getLocalSize()+ringMap0.getLocalSize(), 100000);
    }

//    @Test
    public void testMapNewNode() throws InterruptedException, TimeoutException, ExecutionException {
        ImmutableList<ServiceListBuilder.Constructor> services = new ServiceListBuilder()
                .add("map", bus -> new RingMap<String, Long>(bus, GCounterService::merge, 2)).build();

        Member baseMember = new Member("", 0);
        NoNetworkTransport baseTransport = new NoNetworkTransport(baseMember);

        ProxyJoinerService joinerService = new ProxyJoinerService();

        Cluster cluster0 = new ClusterBuilder().transport(baseTransport::setContext)
                .joinStrategy(joinerService).services(services).serverAddress(baseMember.getAddress())
                .start();

        RingMap ringMap0 = cluster0.getService("map");

        for (int i = 0; i < 100000; i++) {
            ringMap0.put("test" + i, 5).get();
        }

        List<Cluster> newNodes = createFixedFakeCluster(range(1, 3), services).map(e -> {
            e.members().add(baseMember);
            return e.start();
        }).collect(Collectors.toList());

        RingMap ringMap1 = cluster0.getService("map");

        BlockerMigrationListener blocker = new BlockerMigrationListener();
        ringMap1.listenMigrations(blocker);
        newNodes.stream().map(Cluster::getTransport).forEach(tr -> baseTransport.addMember((NoNetworkTransport) tr));
        newNodes.stream().map(Cluster::getLocalMember).forEach(joinerService::addMember);
        blocker.waitForMigrationEnd();

        Thread.sleep(2000);
        System.out.println(1);
//        CompletableFuture<Map<Member, Integer>> size1 = ringMap1.size();
//        Integer size = size1.join().values().stream().reduce((x, y) -> x + y).get();
//        assertEquals(size.intValue(), 2000);
    }

//    @Test
    public void testMapNodeFailure() throws InterruptedException, TimeoutException, ExecutionException {
        ImmutableList<ServiceListBuilder.Constructor> services = new ServiceListBuilder()
                .add("map", bus -> new RingMap<String, Long>(bus, GCounterService::merge, 2)).build();

        Cluster cluster0 = new ClusterBuilder().services(services).start();
        Cluster cluster1 = new ClusterBuilder().services(services).start();
        Cluster cluster2 = new ClusterBuilder().services(services).start();

        waitForDiscovery(cluster0, 3);
        waitForDiscovery(cluster1, 3);
        waitForDiscovery(cluster2, 3);

        RingMap ringMap0 = cluster0.getService("map");

        for (int i = 0; i < 1000; i++) {
            ringMap0.put("test" + i, 5).get();
        }

        cluster2.close();
        waitForMigrationEnd(ringMap0);

        CompletableFuture<Map<Member, Integer>> size1 = ringMap0.size();
        Optional<Integer> test = size1.get().values().stream().reduce((i0, i1) -> i0 + i1);
        assertTrue(test.isPresent());
        assertEquals(test.get().intValue(), 200);
    }

    private void waitForMigrationEnd(RingMap ringMap0) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        ringMap0.listenMigrations(new BlockerMigrationListener());
        countDownLatch.await();
    }

//    @Test
    public void testMapMultipleThreads() throws InterruptedException, TimeoutException, ExecutionException {
        ImmutableList<ServiceListBuilder.Constructor> services = new ServiceListBuilder()
                .add("map", bus -> new RingMap<String, Long>(bus, GCounterService::merge, 2)).build();

        Cluster cluster0 = new ClusterBuilder().services(services).start();
        new ClusterBuilder().services(services).start();

        waitForDiscovery(cluster0, 1);

        RingMap ringMap0 = cluster0.getService("map");
        RingMap ringMap1 = cluster0.getService("map");

        CountDownLatch countDownLatch = new CountDownLatch(2);

        for (int i = 0; i < 10; i++) {
            final int finalI = i;
            new Thread(() -> {
                try {
                    for (int i1 = 0; i1 < 100; i1++) {
                        ringMap0.put("s0" + finalI + i1, finalI + i1).get();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
            }).run();
            new Thread(() -> {
                try {
                    for (int i1 = 0; i1 < 100; i1++) {
                        ringMap1.put("s1" + finalI + i1, finalI + i1).get();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
            }).run();
        }

        countDownLatch.await();
        CompletableFuture<Map<Member, Integer>> size = ringMap1.size();
        Optional<Integer> test = size.get().values().stream().reduce((i0, i1) -> i0 + i1);
        assertTrue(test.isPresent());
        assertEquals(test.get().intValue(), 2000 * 2);
    }

    private static class ProxyJoinerService implements JoinerService {
        public ClusterMembership clusterMembership;

        @Override
        public void onStart(ClusterMembership membership) {
            this.clusterMembership = membership;
        }

        public void addMember(Member member) {
            clusterMembership.addMember(member);
        }
    }

    private static class BlockerMigrationListener implements MigrationListener {

        private CountDownLatch countDownLatch;

        public BlockerMigrationListener() {
            this.countDownLatch = new CountDownLatch(1);
        }

        @Override
        public void migrationStart(Member removedMember) {
        }

        @Override
        public void migrationEnd(Member removedMember) {
            countDownLatch.countDown();
        }

        public void waitForMigrationEnd() throws InterruptedException {
            countDownLatch.await();
            synchronized (this) {
                countDownLatch = new CountDownLatch(1);
            }

        }
    }
}
