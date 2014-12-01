package org.rakam.kume;

import org.rakam.kume.service.ringmap.RingMap;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Created by buremba <Burak Emre Kabakcı> on 28/11/14 16:10.
 */
public class Main {
    public static void main(String[] args) throws MalformedObjectNameException, InterruptedException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        ServiceInitializer services = new ServiceInitializer()
                .add(bus -> new RingMap(bus, 2));

        Cluster cluster0 = new ClusterBuilder().services(services).start();
        mbs.registerMBean(cluster0, new ObjectName("org.rakam.kume:type=Cluster"));
    }
}
