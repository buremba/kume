package org.rakam.kume.service.crdt.counter;

import org.rakam.kume.ServiceContext;
import org.rakam.kume.service.DistributedObjectServiceAdapter;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/12/14 17:56.
 */
public class GCounterService extends DistributedObjectServiceAdapter<GCounterService, Long>
{
    // TODO: Add DistributedLongService for long primitive type?
    // TODO: Move call methods to separate classes and register them with Kryo.

    public GCounterService(ServiceContext clusterContext, Long l, int replicationFactor) {
        super(clusterContext, l, replicationFactor);
    }

    public GCounterService(ServiceContext clusterContext, int replicationFactor) {
        super(clusterContext, 0L, replicationFactor);
    }

    public void add(long l) {
        checkArgument(l > 0, "value (%s) must be a positive integer", l);
        sendToReplicas((service, ctx) -> service.value += l);
    }

    public void increment() {
        sendToReplicas((service, ctx) -> service.value++);
    }

    public void set(long l) {
        checkArgument(l > 0, "value (%s) must be a positive integer", l);
        sendToReplicas((service, ctx) -> service.value = l);
    }

    @Override
    protected boolean mergeIn(Long val) {
        if(val > value) {
            value = val;
            return true;
        }
        return false;
    }

    public static long merge(long val0, long val1) {
        return Long.max(val0, val1);
    }
}
