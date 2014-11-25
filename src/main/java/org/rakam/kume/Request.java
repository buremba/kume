package org.rakam.kume;

import org.rakam.kume.service.Service;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/11/14 18:46.
 */
public interface Request<T extends Service> {
    void run(T service, OperationContext ctx);
}
