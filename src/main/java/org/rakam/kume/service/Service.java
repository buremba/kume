package org.rakam.kume.service;

import org.rakam.kume.Cluster;
import org.rakam.kume.NioEventLoopGroupArray;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 19:04.
 */
public abstract class Service<T extends Service> {
    final static Logger LOGGER = LoggerFactory.getLogger(Service.class);
    protected final Cluster.ServiceContext<T> serviceContext;

    public Cluster.ServiceContext<T> getContext() {
        return serviceContext;
    }

    public Service(Cluster.ServiceContext<T> ctx) {
        this.serviceContext = ctx;
    }

    public void handle(OperationContext ctx, Object object) {
        LOGGER.warn("Discarded message {} because the service doesn't implement handle(OperationContext, Object)", object);
    }

    public void handle(OperationContext ctx, Request request) {
        request.run(this, ctx);
    }

    public void handle(NioEventLoopGroupArray executor, OperationContext ctx, Object object) {
        executor.getChild(serviceContext.serviceId()).execute(() -> handle(ctx, object));
    }

    public void handle(NioEventLoopGroupArray executor, OperationContext ctx, Request request) {
        executor.getChild(serviceContext.serviceId()).execute(() -> request.run(this, ctx));
    }

    public abstract void onClose();
}