package org.rakam.kume.service.crdt.gset;

import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by buremba <Burak Emre Kabakcı> on 28/12/14 22:07.
 */
public class GSet<E> extends ConcurrentSkipListSet<E> {
    public GSet merge(GSet set) {
        addAll(set);
        return this;
    }
    public static GSet merge(GSet set0, GSet set1) {
        set0.merge(set1);
        return set0;
    }
}
