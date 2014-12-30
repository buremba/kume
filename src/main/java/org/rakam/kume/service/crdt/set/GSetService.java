package org.rakam.kume.service.crdt.set;

import org.rakam.kume.Cluster;
import org.rakam.kume.service.DistributedObjectServiceAdapter;

import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 28/12/14 21:26.
 */
public class GSetService<T> extends DistributedObjectServiceAdapter<GSetService<T>, Set<T>> {
    public GSetService(Cluster.ServiceContext clusterContext, Set<T> value, int replicationFactor) {
        super(clusterContext, value, replicationFactor);
    }

    public void add(T entry) {
        sendToReplicas((service, ctx) -> service.value.add(entry));
    }

    public int size() {
        return askReplicas((service, ctx) -> service.value.size(), Integer.class)
                .mapToInt(future -> future.join()).max().getAsInt();
    }

    public boolean contains(T entry) {
        return askReplicas((service, ctx) -> service.value.contains(entry), Boolean.class)
                .anyMatch(f -> f.join().booleanValue());
    }

    @Override
    protected boolean mergeIn(Set<T> val) {
        int size = value.size();
        value.addAll(val);
        return value.size() == size;
    }
}
