package org.rakam.kume;

import io.netty.channel.ChannelHandlerContext;
import org.rakam.kume.transport.Packet;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 19:06.
 */
public class RemoteOperationContext implements OperationContext {
    private final ChannelHandlerContext ctx;
    private final int packageId;
    private final short serviceId;
    private final Member sender;

    public RemoteOperationContext(ChannelHandlerContext ctx, short serviceId, int packageId, Member sender) {
        this.serviceId =  serviceId;
        this.ctx =  ctx;
        this.packageId =  packageId;
        this.sender =  sender;
    }

    public RemoteOperationContext(ChannelHandlerContext ctx, short serviceId, Member sender) {
        this(ctx, serviceId, -1, sender);
    }

    @Override
    public Member getSender() {
        return sender;
    }

    @Override
    public void reply(Object obj) {
        ctx.writeAndFlush(new Packet(packageId, obj, serviceId));
    }

}
