package org.rakam.kume;

import org.junit.Test;
import org.rakam.kume.service.Service;
import org.rakam.kume.service.ServiceInitializer;
import org.rakam.kume.transport.OperationContext;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/11/14 19:34.
 */
public class ClusterTest extends KumeTest {

    @Test
    public void testSendAllMembers() throws InterruptedException {
//        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
//        root.setLevel(Level.TRACE);

        CountDownLatch latch = new CountDownLatch(2);

        ServiceInitializer constructors = new ServiceInitializer()
              .add("test", (bus) -> new MyService(bus, latch));

        Cluster cluster0 = new ClusterBuilder().services(constructors).start();
        Cluster cluster1 = new ClusterBuilder().services(constructors).start();
        Cluster cluster2 = new ClusterBuilder().services(constructors).start();

        waitForDiscovery(cluster0, 2);

        MyService service = cluster0.getService("test");
        service.pingAll();

        latch.await();
    }

    @Test
    public void testListeners() throws InterruptedException {

        ServiceInitializer constructors = new ServiceInitializer()
              .add("test", (bus) -> new SimpleService(bus));

        new ClusterBuilder().services(constructors).start();
    }

    private static class MyService extends Service {

        private final CountDownLatch latch;
        private final ServiceContext<MyService> ctx;

        public MyService(ServiceContext<MyService> bus, CountDownLatch latch) {
            ctx = bus;
            this.latch = latch;
        }

        public void pingAll() {
            ctx.sendAllMembers(1);
        }

        @Override
        public void handle(OperationContext ctx, Object request) {
            if(request.equals(1))
                latch.countDown();
        }

        @Override
        public void onClose() {

        }
    }

    private class SimpleService extends Service implements MembershipListener {
        private SimpleService(ServiceContext cluster) {
            cluster.getCluster().addMembershipListener(this);
        }

        @Override
        public void memberAdded(Member member) {
            System.out.println("memberAdded");
        }

        @Override
        public void memberRemoved(Member member) {
            System.out.println("memberRemoved");
        }

        @Override
        public void clusterMerged(Set<Member> newMembers) {
            System.out.println("clusterMerged");
        }

        @Override
        public void clusterChanged() {
            System.out.println("clusterChanged");
        }

        @Override
        public void onClose() {

        }
    }
}
