package org.rakam.kume.util;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/12/14 01:38.
 */
public class FutureUtil {
    public static class MultipleFutureListener<V> {
        final int expected;
        CompletableFuture<V> f;
        private int current = 0;
        private V val;

        public MultipleFutureListener(int expected) {
            this.expected = expected;
            if(expected > 0)
                f = new CompletableFuture<>();
            else
                f = CompletableFuture.completedFuture(null);
        }

        public static MultipleFutureListener from(Stream<CompletableFuture> stream) {
            MultipleFutureListener l = new MultipleFutureListener((int) stream.count());
            stream.forEach(f -> f.thenAccept((v) -> {
                l.current++;
                l.val = v;
                l.runIfDone();
            }));
            return l;
        }

        public CompletableFuture<V> get() {
            return f;
        }

        public void increment() {
            current++;
            runIfDone();
        }

        private void runIfDone() {
            if (current >= expected)
                f.complete(val);
        }

        public void listen(CompletableFuture<V> f) {
            f.thenAccept((v) -> {
                current++;
                // detect inconsistency
                val = v;
                runIfDone();
            });
        }
    }
}
