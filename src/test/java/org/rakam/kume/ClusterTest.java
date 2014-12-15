package org.rakam.kume;

import org.junit.Test;
import org.rakam.kume.service.Service;

import java.util.concurrent.CountDownLatch;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/11/14 19:34.
 */
public class ClusterTest extends KumeTest {

    @Test
    public void testSendAllMembers() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);

        ServiceInitializer constructors = new ServiceInitializer()
              .add((bus) -> new MyService(bus, latch));

        Cluster cluster0 = new ClusterBuilder().services(constructors).start();
        Cluster cluster1 = new ClusterBuilder().services(constructors).start();
        Cluster cluster2 = new ClusterBuilder().services(constructors).start();

        waitForDiscovery(cluster0, 2);

        MyService service = cluster0.getService(MyService.class);
        service.pingAll();

        latch.await();
    }

    private static class MyService implements Service {

        private final Cluster.ServiceContext bus;
        private final CountDownLatch latch;

        public MyService(Cluster.ServiceContext bus, CountDownLatch latch) {
            this.bus = bus;
            this.latch = latch;
        }

        public void pingAll() {
            bus.sendAllMembers(1);
        }

        @Override
        public void handle(OperationContext ctx, Object request) {
            if(request.equals(1))
                latch.countDown();
        }
    }
}
