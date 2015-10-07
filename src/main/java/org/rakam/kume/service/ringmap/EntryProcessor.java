package org.rakam.kume.service.ringmap;


public interface EntryProcessor<T, R> {
    default void set(T value) {

    }
    R apply(T t);
}
