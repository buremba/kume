package org.rakam.kume.transport;

import org.rakam.kume.service.Service;

import java.io.Serializable;


@FunctionalInterface
public interface Request<T extends Service, R> extends Serializable {
    void run(T service, OperationContext<R> ctx);
}
