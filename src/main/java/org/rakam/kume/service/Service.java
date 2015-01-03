package org.rakam.kume.service;

import org.rakam.kume.NioEventLoopGroupArray;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 19:04.
 */
public abstract class Service {
    final static Logger LOGGER = LoggerFactory.getLogger(Service.class);

    public void handle(OperationContext ctx, Object object) {
        LOGGER.warn("Discarded message {} because the service doesn't implement handle(OperationContext, Object)", object);
    }

    public void handle(OperationContext ctx, Request request) {
        request.run(this, ctx);
    }

    public void handle(NioEventLoopGroupArray executor, OperationContext ctx, Object object) {
        executor.getChild(ctx.serviceId() % executor.executorCount()).execute(() -> handle(ctx, object));
    }

    public void handle(NioEventLoopGroupArray executor, OperationContext ctx, Request request) {
        executor.getChild(ctx.serviceId() % executor.executorCount()).execute(() -> request.run(this, ctx));
    }

    public abstract void onClose();
}