package org.rakam.kume;

import org.rakam.kume.service.ServiceInitializer;
import org.rakam.kume.util.NetworkUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/11/14 19:53.
 */
public class ClusterBuilder {
    private Collection<Member> members;
    private ServiceInitializer services;
    private InetSocketAddress serverAddress;
    private boolean mustJoinCluster;
    private boolean client = false;

    public ClusterBuilder members(Collection<Member> members) {
        this.members = members;
        return this;
    }

    public Collection<Member> members() {
        return members;
    }

    public ClusterBuilder services(ServiceInitializer services) {
        this.services = services;
        return this;
    }

    public boolean mustJoinCluster() {
        return mustJoinCluster;
    }

    public boolean client() {
        return client;
    }

    public ClusterBuilder client(boolean client) {
        this.client = client;
        return this;
    }

    public ClusterBuilder mustJoinCluster(boolean join) {
        mustJoinCluster = join;
        return this;
    }

    public ServiceInitializer services() {
        return services;
    }

    public ClusterBuilder serverAddress(InetSocketAddress serverAddress) {
        this.serverAddress = serverAddress;
        return this;
    }

    public InetSocketAddress serverAddress() {
        return serverAddress;
    }

    public Cluster start() {
        if (members == null)
            members = new ArrayList<>();

        if (serverAddress == null)
            serverAddress = new InetSocketAddress(NetworkUtil.getDefaultAddress(), 0);

        if(services==null)
            services = new ServiceInitializer();

        return new Cluster(members, services, serverAddress, mustJoinCluster, client);
    }
}
