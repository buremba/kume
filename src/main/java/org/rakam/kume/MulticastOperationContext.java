package org.rakam.kume;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/12/14 19:38.
 */
public class MulticastOperationContext implements OperationContext {
    Member sender;

    public MulticastOperationContext(Member sender) {
        this.sender = sender;
    }

    @Override
    public void reply(Object obj) {
        throw new IllegalAccessError("reply is not supported for multicast operations");
    }

    @Override
    public Member getSender() {
        return sender;
    }
}
