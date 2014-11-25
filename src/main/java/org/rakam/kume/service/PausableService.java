package org.rakam.kume.service;

import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 02:12.
 */
public abstract class PausableService implements Service {
    ArrayDeque<FutureRequest> objectQueue = new ArrayDeque();
    private boolean paused  = false;

    @Override
    public void handle(OperationContext ctx, Object object) {
        if(paused) {
            objectQueue.add(new FutureRequest(ctx, object));
        }else {
            safelyHandle(ctx, object);
        }
    }

    protected abstract void safelyHandle(OperationContext ctx, Object object);
    protected abstract CompletionStage<Object> safelyHandleRequest(CompletableFuture f, Request request);

    public synchronized void pause() {
        paused = true;
    }

    public void resume() {
        objectQueue.forEach(x -> safelyHandle(x.context, x.request));
        paused = false;
    }

    public static class FutureRequest {
        OperationContext context;
        Object request;

        public FutureRequest(OperationContext context, Object request) {
            this.context = context;
            this.request = request;
        }
    }
}
