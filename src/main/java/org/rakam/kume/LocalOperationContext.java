package org.rakam.kume;

import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/12/14 23:05.
 */
public class LocalOperationContext<R> implements OperationContext<R> {
    private final CompletableFuture<R> callback;
    private final Member member;

    public LocalOperationContext(CompletableFuture<R> callback, Member localMember) {
        this.callback = callback;
        this.member = localMember;
    }

    @Override
    public void reply(R obj) {
        callback.complete(obj);
    }

    @Override
    public Member getSender() {
        return member;
    }
}
