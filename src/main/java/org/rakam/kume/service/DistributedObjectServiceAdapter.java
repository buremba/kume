package org.rakam.kume.service;

import org.rakam.kume.ServiceContext;

import java.util.function.Consumer;


public abstract class DistributedObjectServiceAdapter<C extends DistributedObjectServiceAdapter<C, T>, T> extends DistributedObjectService<C, T> {
    protected T value;

    public DistributedObjectServiceAdapter(ServiceContext clusterContext, T value, int replicationFactor) {
        super(clusterContext, replicationFactor);
        this.value = value;
    }

    public void process(Consumer<T> processor) {
        getOwnedMembers()
                .forEach(member -> getContext()
                        .send(member, (service, ctx) -> processor.accept(service.value)));
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
