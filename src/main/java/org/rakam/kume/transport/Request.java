package org.rakam.kume.transport;

import org.rakam.kume.service.Service;
import org.rakam.kume.transport.OperationContext;

import java.io.Serializable;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/11/14 18:46.
 */
public interface Request<T extends Service, R> extends Serializable {
    void run(T service, OperationContext<R> ctx);
}
