package org.rakam.kume.transport;

import org.rakam.kume.Member;


public interface OperationContext<R> {
    void reply(R obj);
    Member getSender();
    int serviceId();
}
