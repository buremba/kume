package org.rakam.kume.service;

import org.rakam.kume.Operation;
import org.rakam.kume.Request;

import java.util.ArrayDeque;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 02:12.
 */
public abstract class PausableService implements Service {
    ArrayDeque queue = new ArrayDeque();
    private boolean paused  = false;

    @Override
    public void handleOperation(Operation operation) {

    }

    @Override
    public Object handleRequest(Request request) {
        return null;
    }

    @Override
    public void onStart() {

    }

    @Override
    public void onClose() {

    }

    public synchronized void pause() {
        paused = true;
    }

    public void resume() {
        queue.forEach(x -> {
            if(x instanceof Operation)
                handleOperation((Operation) x);
        });
        paused = false;
    }
}
