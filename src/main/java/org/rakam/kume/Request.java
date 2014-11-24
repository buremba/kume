package org.rakam.kume;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/11/14 18:46.
 */
public interface Request<V> {
    V run();
    public int getService();
}
