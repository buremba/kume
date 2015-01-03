package org.rakam.kume.transport;

import org.rakam.kume.Member;

import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/12/14 23:05.
 */
public class LocalOperationContext<R> implements OperationContext<R> {
    private final CompletableFuture<R> callback;
    private final Member member;
    private final int serviceId;

    public LocalOperationContext(CompletableFuture<R> callback, int serviceId, Member localMember) {
        this.callback = callback;
        this.serviceId = serviceId;
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

    @Override
    public int serviceId() {
        return serviceId;
    }
}
