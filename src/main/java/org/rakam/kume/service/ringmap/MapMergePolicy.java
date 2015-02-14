package org.rakam.kume.service.ringmap;

import java.io.Serializable;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/12/14 23:57.
 */
public interface MapMergePolicy<V> extends Serializable {
    public V merge(V first, V other);
}
