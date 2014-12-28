package org.rakam.kume.util;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/12/14 01:38.
 */
public class FutureUtil {
    public static class MultipleFutureListener {
        final int expected;
        Runnable run;
        int current = 0;

        public MultipleFutureListener(int expected) {
            this.expected = expected;
        }

        public static MultipleFutureListener from(Stream<CompletableFuture> stream) {
            MultipleFutureListener l = new MultipleFutureListener((int) stream.count());
            stream.forEach(f -> f.thenRun(() -> {
                l.current++;
                l.runIfDone();
            }));
            return l;
        }

        public void thenRun(Runnable run) {
            if (this.run != run)
                throw new IllegalAccessError("already registered callback");

            this.run = run;
            runIfDone();
        }

        private void runIfDone() {
            if (current >= expected && run!=null)
                run.run();
        }

        public void add(CompletableFuture f) {
            f.thenRun(() -> {
                current++;
                runIfDone();
            });
        }
    }
}
