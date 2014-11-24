package org.rakam.kume;

import org.rakam.kume.service.Service;
import org.rakam.kume.service.ServiceConstructor;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/11/14 19:53.
 */
public class ClusterBuilder {
    private Collection<Member> members;
    private List<ServiceConstructor<Service>> services;
    private InetSocketAddress serverAddress;

    public ClusterBuilder setMembers(Collection<Member> members) {
        this.members = members;
        return this;
    }

    public Collection<Member> getMembers() {
        return members;
    }

    public ClusterBuilder setServices(List<ServiceConstructor<Service>> services) {
        this.services = services;
        return this;
    }

    public List<ServiceConstructor<Service>> getServices() {
        return services;
    }

    public ClusterBuilder setServerAddress(InetSocketAddress serverAddress) {
        this.serverAddress = serverAddress;
        return this;
    }

    public InetSocketAddress getServerAddress() {
        return serverAddress;
    }

    public Cluster start() throws InterruptedException {
        Cluster cluster = new Cluster(members, services);
        if(serverAddress==null)
            cluster.start();
        else
            cluster.start(serverAddress);
        return cluster;
    }
}
