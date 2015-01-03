package org.rakam.kume.util;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/12/14 05:59.
 */
public class NioEventLoopGroupArray extends NioEventLoopGroup {
    EventExecutor[] children;

    public NioEventLoopGroupArray() {
        super();
        children = children().stream().toArray(EventExecutor[]::new);
    }

    public EventExecutor getChild(int id) {
        return children[id % children.length];
    }
}
