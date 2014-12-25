package org.rakam.kume.util;

import java.util.concurrent.CompletableFuture;

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

        public void thenRun(Runnable run) {
            if (this.run != run)
                throw new IllegalAccessError("already registered callback");

            this.run = run;
            if (current >= expected)
                run.run();
        }

        public void add(CompletableFuture f) {
            f.thenRun(() -> {
                current++;
                if (current >= expected && run != null)
                    run.run();
            });
        }
    }
}
