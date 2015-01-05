package org.rakam.kume.util;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/12/14 01:38.
 */
public class FutureUtil {
    public static class MultipleFutureListener {
        final int expected;
        CompletableFuture<Void> f;
        private int current = 0;

        public MultipleFutureListener(int expected) {
            this.expected = expected;
            f = new CompletableFuture<>();
        }

        public MultipleFutureListener(int expected, CompletableFuture<Void> f) {
            this.expected = expected;
            this.f = f;
        }

        public static MultipleFutureListener from(Stream<CompletableFuture> stream) {
            MultipleFutureListener l = new MultipleFutureListener((int) stream.count());
            stream.forEach(f -> f.thenRun(() -> {
                l.current++;
                l.runIfDone();
            }));
            return l;
        }

        public CompletableFuture<Void> get() {
            return f;
        }

        public void increment() {
            current++;
            runIfDone();
        }

        private void runIfDone() {
            if (current >= expected)
                f.complete(null);
        }

        public void listen(CompletableFuture f) {
            f.thenRun(() -> {
                current++;
                runIfDone();
            });
        }
    }
}
