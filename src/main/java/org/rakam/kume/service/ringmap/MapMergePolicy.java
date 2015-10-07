package org.rakam.kume.service.ringmap;

import java.io.Serializable;


public interface MapMergePolicy<V> extends Serializable {
    public V merge(V first, V other);
}
