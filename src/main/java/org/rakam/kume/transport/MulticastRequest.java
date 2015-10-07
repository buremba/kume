package org.rakam.kume.transport;

import org.rakam.kume.Member;
import org.rakam.kume.service.Service;


public abstract class MulticastRequest<T extends Service, R> implements Request<T, R> {
    Member sender;

    protected MulticastRequest(Member sender) {
        this.sender = sender;
    }
}
