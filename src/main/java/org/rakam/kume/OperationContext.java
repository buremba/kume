package org.rakam.kume;

import io.netty.channel.ChannelHandlerContext;
import org.rakam.kume.transport.Packet;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 19:06.
 */
public class OperationContext {
    private final ChannelHandlerContext ctx;
    private final int packageId;
    private final short serviceId;

    public OperationContext(ChannelHandlerContext ctx, short serviceId, int packageId) {
        this.serviceId =  serviceId;
        this.ctx =  ctx;
        this.packageId =  packageId;
    }

    public OperationContext(ChannelHandlerContext ctx, short serviceId) {
        this(ctx, serviceId, -1);
    }

    public Member getSender() {
        return null;
    }

    public void reply(Object obj) {
        ctx.writeAndFlush(new Packet(packageId, obj, serviceId));
    }

}
