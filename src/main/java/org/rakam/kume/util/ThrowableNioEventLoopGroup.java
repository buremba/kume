package org.rakam.kume.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;

import java.lang.Thread.UncaughtExceptionHandler;


public class ThrowableNioEventLoopGroup extends NioEventLoopGroup {
    EventExecutor[] children;


    public ThrowableNioEventLoopGroup(int nThreads, String name, UncaughtExceptionHandler exceptionHandler) {
        super(nThreads, new ThreadFactoryBuilder()
                .setNameFormat(name + "-%d")
                .setUncaughtExceptionHandler(exceptionHandler)
                .build());
        children = super.children().stream().toArray(EventExecutor[]::new);
    }

    public ThrowableNioEventLoopGroup(String name, UncaughtExceptionHandler exceptionHandler) {
        this(Runtime.getRuntime().availableProcessors()*2, name, exceptionHandler);
    }

    public EventExecutor getChild(int id) {
        return children[id % children.length];
    }

}
