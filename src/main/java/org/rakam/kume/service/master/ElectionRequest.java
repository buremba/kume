package org.rakam.kume.service.master;

import org.rakam.kume.Request;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 02:06.
 */
public interface ElectionRequest<T> extends Request<T> {
    @Override
    default int getService() {
        return 2;
    }
}
