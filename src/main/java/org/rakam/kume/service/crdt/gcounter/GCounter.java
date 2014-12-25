package org.rakam.kume.service.crdt.gcounter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.concurrent.atomic.LongAdder;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;

/**
 * Created by buremba <Burak Emre Kabakcı> on 20/12/14 03:22.
 */
public class GCounter implements KryoSerializable {
    LongAdder counter = new LongAdder();

    public GCounter(long l) {
        // there is no way to construct LongAdder with an initial value.
        // since write performance is much more better than AtomicLong,
        // this is not actually big issue.
        this.counter = new LongAdder();
        counter.add(l);
    }

    public GCounter() {
    }

    public void increment() {
        counter.increment();
    }

    public void add(long l) {
        checkArgument(l > 0, "value (%s) must be a positive integer", l);
        counter.add(l);
    }

    public long get() {
        return counter.longValue();
    }

    public static GCounter combine(GCounter first, GCounter second) {
        long max = max(first.counter.longValue(), second.counter.longValue());
        return new GCounter(max);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeLong(counter.longValue(), true);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        long l = input.readLong(true);
        counter = new LongAdder();
        counter.add(l);
    }


}
