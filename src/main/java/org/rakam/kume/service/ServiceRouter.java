package org.rakam.kume.service;

import org.rakam.kume.Cluster;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/11/14 20:51.
 */
public abstract class ServiceRouter implements Service {
    Service[] subServices;

    public ServiceRouter(Cluster.ServiceContext bus, int count, ServiceConstructor constructor) {
        subServices = Stream.generate(() -> constructor.newInstance(bus)).limit(8)
                .toArray(value -> new Service[count]);
    }

    @Override
    public void handle(OperationContext ctx, Object request) {
        subServices[getServiceId(request)].handle(ctx, request);
    }

    @Override
    public void handle(OperationContext ctx, Request request) {
        subServices[getServiceId(request)].handle(ctx, request);
    }

    @Override
    public void onStart() {
        Arrays.stream(subServices).forEach(x -> x.onStart());
    }

    @Override
    public void onClose() {
        Arrays.stream(subServices).forEach(x -> x.onClose());
    }

    abstract int getServiceId(Object obj);
    abstract int getServiceId(Request request);
}
