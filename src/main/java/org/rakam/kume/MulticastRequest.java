package org.rakam.kume;

import org.rakam.kume.service.Service;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/12/14 19:01.
 */
public abstract class MulticastRequest<T extends Service, R> implements Request<T, R> {
    Member sender;

    protected MulticastRequest(Member sender) {
        this.sender = sender;
    }
}
