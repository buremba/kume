package org.rakam.kume.util;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/12/14 05:59.
 */
public class NioEventLoopGroupArray extends NioEventLoopGroup {
    EventExecutor[] children;


    public NioEventLoopGroupArray(int nThreads, String name, UncaughtExceptionHandler exceptionHandler) {
        super(nThreads, new ExceptionCatchingThreadFactory(name, exceptionHandler));
        children = super.children().stream().toArray(EventExecutor[]::new);
    }

    public NioEventLoopGroupArray(String name, UncaughtExceptionHandler exceptionHandler) {
        this(Runtime.getRuntime().availableProcessors()*2, name, exceptionHandler);
    }

    public EventExecutor getChild(int id) {
        return children[id % children.length];
    }

    private static class ExceptionCatchingThreadFactory implements ThreadFactory {
        private final AtomicInteger id = new AtomicInteger();
        private final UncaughtExceptionHandler exceptionHandler;
        private String name;

        public ExceptionCatchingThreadFactory(String name, UncaughtExceptionHandler exceptionHandler) {
            this.name = name;
            this.exceptionHandler = exceptionHandler;
        }

        public Thread newThread(final Runnable r) {
            Thread t = new Thread(r, name+"-"+id.getAndIncrement());
            t.setUncaughtExceptionHandler(exceptionHandler);
            return t;
        }
    }
}
