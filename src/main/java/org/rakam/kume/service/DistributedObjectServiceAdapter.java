package org.rakam.kume.service;

import org.rakam.kume.Cluster;

import java.util.function.Consumer;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/12/14 18:39.
 */
public abstract class DistributedObjectServiceAdapter<C extends DistributedObjectServiceAdapter<C, T>, T> extends DistributedObjectService<C, T> {
    protected T value;

    public DistributedObjectServiceAdapter(Cluster.ServiceContext clusterContext, T value, int replicationFactor) {
        super(clusterContext, replicationFactor);
        this.value = value;
    }

    public void process(Consumer<T> processor) {
        getOwnedMembers().forEach(member -> serviceContext.send(member, (service, ctx) -> processor.accept(service.value)));
    }

    @Override
    public T getLocal() {
        return value;
    }

    @Override
    public void setLocal(T val) {
        value = val;
    }

    @Override
    public void onClose() {

    }

    @Override
    protected abstract boolean mergeIn(T val);
}
