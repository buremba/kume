package org.rakam.kume.service.ringmap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/02/15 17:58.
 */
public class Modifiable<K> {
    private K value;
    private boolean changed;

    public Modifiable(K item) {
        this.value = item;
    }

    public K value() {
        return value;
    }

    public boolean changed() {
        return changed;
    }

    public void value(K value) {
        this.value = value;
        changed = true;
    }
}
