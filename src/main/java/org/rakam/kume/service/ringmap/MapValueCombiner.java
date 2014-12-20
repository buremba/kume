package org.rakam.kume.service.ringmap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/12/14 22:39.
 */
public interface MapValueCombiner<T extends MapValueCombiner> {
    public T combine(T first, T second);
}