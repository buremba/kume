package org.rakam.kume;

import io.netty.channel.ChannelHandlerContext;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 19:06.
 */
public class OperationContext {
    private Member sender;
    private int packageId;

    public OperationContext(Cluster eventBus, ChannelHandlerContext ctx, int packageId) {

    }

    public OperationContext(Cluster eventBus, ChannelHandlerContext ctx) {

    }

    public Member getSender() {
        return sender;
    }
    public void reply(Object obj) {

    }

}
