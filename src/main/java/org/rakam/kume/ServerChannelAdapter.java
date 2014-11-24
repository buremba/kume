package org.rakam.kume;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.rakam.kume.service.Service;
import org.rakam.kume.transport.Packet;
import org.rakam.kume.transport.PacketDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/11/14 02:34.
 */
public class ServerChannelAdapter extends ChannelInboundHandlerAdapter {
    List<Service> services;
    Cluster eventBus;
    final static Logger LOGGER = LoggerFactory.getLogger(PacketDecoder.class);

    public ServerChannelAdapter(Cluster bus) {
        this.services = bus.getServices();
        this.eventBus = bus;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Packet read = (Packet) msg;
        Object o = read.getData();
        if (o instanceof Operation) {
            Operation o1 = (Operation) o;
            int service = o1.getService();
            if (service == -1)
                eventBus.handleOperation(o1);
            Service service1 = services.get(service);
            try {
                service1.handleOperation(o1);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        } else if (o instanceof Request) {
            Request o1 = (Request) o;
            int service = o1.getService();
            if (service == -1)
                eventBus.handleRequest(o1);
            Service service1 = services.get(service);
            Object o2;
            try {
                o2 = service1.handleRequest(o1);
            } catch (Throwable e) {
                o2 = null;
                e.printStackTrace();
            }

            ctx.write(new Packet(read.packetNum, o2));
        } else {
            LOGGER.error("Unknown message type received {}", o.getClass().getCanonicalName());
        }
    }
}
