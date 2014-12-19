package org.rakam.kume;

import io.netty.channel.ChannelHandlerContext;
import org.rakam.kume.transport.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 19:06.
 */
public class RemoteOperationContext implements OperationContext {
    final static Logger LOGGER = LoggerFactory.getLogger(RemoteOperationContext.class);

    private final ChannelHandlerContext ctx;
    private final int packageId;
    private final int serviceId;
    private final Cluster cluster;

    public RemoteOperationContext(ChannelHandlerContext ctx, int serviceId, int packageId, Cluster cluster) {
        this.serviceId =  serviceId;
        this.ctx =  ctx;
        this.packageId =  packageId;
        this.cluster =  cluster;
    }

    @Override
    public Member getSender() {
        // i don't want to include sender identity to each message
        // so we need a clever way to find the sender.
        // TODO: maybe we can keep a reverse map to resolve sender?

//        Optional<Member> first = cluster.getMembers().stream()
//                .filter(x -> x.getAddress().equals(ctx.channel().remoteAddress()))
//                .findFirst();
//        return first.get();
        return null;
    }

    @Override
    public void reply(Object obj) {
        Packet msg = new Packet(packageId, obj, serviceId);
        LOGGER.trace("Answering package {}", msg);

        ctx.writeAndFlush(msg);
    }

}
