package org.rakam.kume;

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

    public Cluster start() throws InterruptedException {
        if (members == null)
            members = new ArrayList<>();

        if (services == null)
            throw new IllegalArgumentException("services are not set");

        if (serverAddress != null)
            return new Cluster(members, services, serverAddress);
        else
            return new Cluster(members, services);

    }
}
