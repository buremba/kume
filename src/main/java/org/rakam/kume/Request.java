package org.rakam.kume;

import org.rakam.kume.service.Service;

import java.io.Serializable;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/11/14 18:46.
 */
public interface Request<T extends Service> extends Serializable {
    void run(T service, OperationContext ctx);
}
