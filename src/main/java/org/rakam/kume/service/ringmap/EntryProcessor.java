package org.rakam.kume.service.ringmap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/02/15 17:38.
 */
public interface EntryProcessor<T, R> {
    default void set(T value) {

    }
    R apply(T t);
}
