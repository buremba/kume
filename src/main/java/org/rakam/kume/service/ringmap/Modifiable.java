package org.rakam.kume.service.ringmap;


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
