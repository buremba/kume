package org.rakam.kume;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/12/14 23:06.
 */
public interface OperationContext<R> {
    public void reply(R obj);
    public Member getSender();
    int serviceId();
}
