package org.rakam.kume.service.ringmap;

import org.rakam.kume.Cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class RingMap<K, V> extends AbstractRingMap<RingMap, Map, K ,V> {
    public RingMap(Cluster.ServiceContext<RingMap> serviceContext, Supplier<Map> mapSupplier, MapMergePolicy<V> mergePolicy, int replicationFactor) {
        super(serviceContext, mapSupplier, mergePolicy, replicationFactor);
    }

    public RingMap(Cluster.ServiceContext<RingMap> serviceContext, MapMergePolicy<V> mergePolicy, int replicationFactor) {
        super(serviceContext, ConcurrentHashMap::new, mergePolicy, replicationFactor);
    }
}