package org.rakam.kume;

import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/12/14 23:05.
 */
public class LocalOperationContext implements OperationContext {
    private final CompletableFuture<Result> callback;

    public LocalOperationContext(CompletableFuture<Result> callback) {
        this.callback = callback;
    }

    @Override
    public void reply(Object obj) {
        callback.complete(new Result(obj));
    }

    @Override
    public Member getSender() {
        return null;
    }
}
