package org.rakam.kume.service;

import org.rakam.kume.OperationContext;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 19:04.
 */
public interface Service {
    void handle(OperationContext ctx, Object request);

    void onStart();
    void onClose();
}