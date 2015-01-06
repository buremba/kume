package org.rakam.kume.util;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/01/15 00:03.
 */
public class NotImplementedException extends RuntimeException {
    public NotImplementedException() {
    }

    public NotImplementedException(String message) {
        super(message);
    }
}
