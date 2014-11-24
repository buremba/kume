package org.rakam.kume;

import org.junit.Test;
import org.rakam.kume.service.Service;
import org.rakam.kume.service.ServiceConstructor;
import org.rakam.kume.util.NetworkUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/11/14 19:34.
 */
public class ClusterTest {

    @Test
    public void voteTest() throws InterruptedException {
        ArrayList<Member> members = new ArrayList();
        ArrayList<ServiceConstructor<Service>> services = new ArrayList<>();
        InetSocketAddress serverAddress = new InetSocketAddress(NetworkUtil.getDefaultAddress(), 0);

        Cluster cluster0 = new ClusterBuilder()
        .setMembers(members)
        .setServices(services)
        .setServerAddress(serverAddress)
        .start();

        Cluster cluster1 = new ClusterBuilder()
        .setMembers(members)
        .setServices(services)
        .setServerAddress(serverAddress)
        .start();

        cluster0.voteElection().thenAcceptAsync((result) -> {
            System.out.println("is master" + result);
        });
    }
}
