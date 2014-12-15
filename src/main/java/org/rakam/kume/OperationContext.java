package org.rakam.kume;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/12/14 23:06.
 */
public interface OperationContext {
    public void reply(Object obj);
    public Member getSender();
}
