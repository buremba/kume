package org.rakam.kume;

import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/12/14 23:05.
 */
public class LocalOperationContext implements OperationContext {
    private final CompletableFuture<Result> callback;
    private final Member member;

    public LocalOperationContext(CompletableFuture<Result> callback, Member localMember) {
        this.callback = callback;
        this.member = localMember;
    }

    @Override
    public void reply(Object obj) {
        callback.complete(new Result(obj));
    }

    @Override
    public Member getSender() {
        return member;
    }
}
