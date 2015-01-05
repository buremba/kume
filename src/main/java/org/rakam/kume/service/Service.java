package org.rakam.kume.service;

import io.netty.util.concurrent.EventExecutor;
import org.rakam.kume.transport.OperationContext;
import org.rakam.kume.transport.Request;
import org.rakam.kume.util.NioEventLoopGroupArray;
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

    public void handle(NioEventLoopGroupArray executor, OperationContext ctx, Object object) {
        int id = ctx.serviceId() % executor.executorCount();
        EventExecutor child = executor.getChild(id);
        if(child.inEventLoop()) {
            try {
                handle(ctx, object);
            } catch (Exception e) {
                LOGGER.error("error while running throwable code block", e);
                // TODO: should we reply or wait for timeout?
            }
        } else {
            child.execute(() -> handle(ctx, object));
        }
    }

    public void handle(NioEventLoopGroupArray executor, OperationContext ctx, Request request) {
        int id = ctx.serviceId() % executor.executorCount();
        EventExecutor child = executor.getChild(id);
        if(child.inEventLoop()) {
            // no need to create new runnable, just execute the request since we're already in event thread.
            try {
                request.run(this, ctx);
            } catch (Exception e) {
                LOGGER.error("error while running throwable code block", e);
                // TODO: should we reply or wait for timeout?
            }
        } else {
            child.execute(() -> request.run(this, ctx));
        }
    }

    public abstract void onClose();
}