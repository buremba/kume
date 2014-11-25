package org.rakam.kume;

import org.junit.Test;
import org.rakam.kume.service.ringmap.RingMap;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/11/14 19:07.
 */
public class RingMapTest extends KumeTest {

    @Test
    public void testMap() throws InterruptedException, TimeoutException, ExecutionException {
        ServiceInitializer services = new ServiceInitializer()
            .add(bus -> new RingMap(bus));

        Cluster cluster0 = new ClusterBuilder().setServices(services).start();
        Cluster cluster1 = new ClusterBuilder().setServices(services).start();

        RingMap ringMap0 = cluster0.getService(RingMap.class);
        RingMap ringMap1 = cluster0.getService(RingMap.class);

        ringMap0.put("test", 5);


        Object test = ((CompletableFuture<Result>) ringMap1.get("test")).get(10, TimeUnit.SECONDS).getData();
        assertEquals(test, 5);
    }
}
