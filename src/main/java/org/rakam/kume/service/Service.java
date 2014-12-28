package org.rakam.kume.service;

import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 19:04.
 */
public interface Service {
    final static Logger LOGGER = LoggerFactory.getLogger(Service.class);

    default void handle(OperationContext ctx, Object object) {
        LOGGER.warn("Discarded message {} because the service doesn't implement handle(OperationContext, Object)", object);
    }

    default void handle(OperationContext ctx, Request request) {
        request.run(this, ctx);
    }

    default void onClose() {
    }

    default void destroy() {
    }
}