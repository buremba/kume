package org.rakam.kume;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.rakam.kume.service.Service;
import org.rakam.kume.service.ServiceListBuilder;
import org.rakam.kume.transport.OperationContext;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.collect.ImmutableList.of;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/11/14 19:34.
 */
public class ClusterTest extends KumeTest {

    @Test
    public void testSendAllMembers() throws InterruptedException {
        Member member0 = new Member("", 0);
        Member member1 = new Member("", 1);
        Member member2 = new Member("", 2);

        CountDownLatch latch = new CountDownLatch(2);

        List<NoNetworkTransport> buses = of(new NoNetworkTransport(member0),
                                            new NoNetworkTransport(member1),
                                            new NoNetworkTransport(member2));

        buses.stream().forEach(bus -> buses.stream().filter(other -> !other.equals(bus)).forEach(bus::addMember));

        ImmutableList<ServiceListBuilder.Constructor> services =
                new ServiceListBuilder().add("test", (bus) -> new PingService(bus, latch)).build();

        Cluster cluster0 = new ClusterBuilder()
                .joinStrategy(new DelayedJoinerService(of(member1, member2), Duration.ofSeconds(1)))
                .transport(buses.get(0)::setContext).services(services).serverAddress(member0.getAddress())
                .start();
        Cluster cluster1 = new ClusterBuilder()
                .joinStrategy(new DelayedJoinerService(of(member0, member2), Duration.ofSeconds(1)))
                .transport(buses.get(1)::setContext).services(services).serverAddress(member1.getAddress())
                .start();
        Cluster cluster2 = new ClusterBuilder()
                .joinStrategy(new DelayedJoinerService(of(member0, member1), Duration.ofSeconds(1)))
                .transport(buses.get(2)::setContext).services(services).serverAddress(member2.getAddress())
                .start();

        waitForDiscovery(cluster0, 2);

        PingService service = cluster0.getService("test");

        service.pingAll();

        latch.await();
    }

    private static class PingService extends Service {

        private final CountDownLatch latch;
        private final ServiceContext<PingService> ctx;

        public PingService(ServiceContext<PingService> bus, CountDownLatch latch) {
            ctx = bus;
            this.latch = latch;
        }

        public void pingAll() {
            ctx.sendAllMembers(1);
        }

        @Override
        public void handle(OperationContext ctx, Object request) {
            latch.countDown();
        }

        @Override
        public void onClose() {

        }
    }

}
