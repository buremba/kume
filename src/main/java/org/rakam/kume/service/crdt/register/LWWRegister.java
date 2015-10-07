package org.rakam.kume.service.crdt.register;

import org.rakam.kume.service.DistributedObjectServiceAdapter;
import org.rakam.kume.ServiceContext;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


public class LWWRegister<T> extends DistributedObjectServiceAdapter<LWWRegister<T>, LWWRegister.ObjectTimestampHolder<T>>
{

    public LWWRegister(ServiceContext clusterContext, T value, int replicationFactor) {
        super(clusterContext, new LWWRegister.ObjectTimestampHolder<>(value), replicationFactor);
    }

    public void set(T entry) {
        ObjectTimestampHolder<T> holder = new ObjectTimestampHolder<>(entry);
        sendToReplicas((service, ctx) -> {
            if(service.value.timestamp < holder.timestamp) {
                service.value = holder;
            }
        });
    }

    public T get() {
        Stream<CompletableFuture<ObjectTimestampHolder<T>>> stream = askReplicas((service, ctx) -> service.get());
        return stream.max((o1, o2) -> Long.compare(o1.join().timestamp, o2.join().timestamp))
                .get().join().value;
    }

    @Override
    protected boolean mergeIn(ObjectTimestampHolder<T> val) {
        if(val.timestamp > value.timestamp) {
            value = val;
            return true;
        }
        return false;
    }

    // we do not use vector clocks because they're expensive.
    public static class ObjectTimestampHolder<T> {
        T value;
        long timestamp;

        public ObjectTimestampHolder(T value) {
            this.value = value;
            // TODO: clock synchronization using master node or a public time server?
            timestamp = System.currentTimeMillis();
        }
    }
}