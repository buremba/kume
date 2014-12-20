package org.rakam.kume.service.ringmap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/12/14 23:57.
 */
public interface MapMergePolicy<V> {
    public V merge(V first, V other);
}
