package org.rakam.kume.service;

import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 19:04.
 */
public interface Service {
    void handle(OperationContext ctx, Object request);

    default void handle(OperationContext ctx, Request request) {
        request.run(this, ctx);
    }

    default void onClose() {
    }
}