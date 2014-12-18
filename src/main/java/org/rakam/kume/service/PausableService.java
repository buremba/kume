package org.rakam.kume.service;

import org.rakam.kume.OperationContext;

import java.util.ArrayDeque;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 02:12.
 */
public abstract class PausableService implements Service {
    ArrayDeque<FutureRequest> objectQueue = new ArrayDeque();
    ArrayDeque<Runnable> runnableQueue = new ArrayDeque();
    private volatile boolean paused  = false;

    @Override
    public void handle(OperationContext ctx, Object object) {
        if(paused) {
            LOGGER.debug("Queued message {} for paused service {}", object, this);
            objectQueue.add(new FutureRequest(ctx, object));
        }else {
            safelyHandle(ctx, object);
        }
    }

    public boolean addQueueIfPaused(Runnable run) {
        if(paused) {
            LOGGER.trace("Queued runnable {} for paused service {}", run, this);
            runnableQueue.add(run);
            return true;
        }
        return false;
    }


    public boolean isPaused() {
        return paused;
    }


    public void safelyHandle(OperationContext ctx, Object object) {
        LOGGER.warn("Discarded message {} because the service doesn't implement handle(OperationContext, request)", object);
    }

    public synchronized void pause() {
        LOGGER.debug("Paused service {}", this);
        paused = true;
    }

    public synchronized void resume() {
        LOGGER.debug("Resumed service {}", this);
        objectQueue.forEach(x -> safelyHandle(x.context, x.request));
        runnableQueue.forEach(x -> x.run());
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
