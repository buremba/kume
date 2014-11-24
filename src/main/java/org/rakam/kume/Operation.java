package org.rakam.kume;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/11/14 18:44.
 */
public interface Operation extends Request<Void> {

    default Void run() {
        execute();
        return null;
    }

    abstract public void execute();

    abstract public int getService();
}
