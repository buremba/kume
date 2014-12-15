package org.rakam.kume.util;

/**
 * Created by buremba <Burak Emre Kabakcı> on 13/12/14 14:49.
 */
public class Tuple<A, B> {
    final public A _1;
    final public B _2;

    public Tuple(A _1, B _2) {
        this._1 = _1;
        this._2 = _2;
    }

    public A _1() {
        return _1;
    }

    public B _2() {
        return _2;
    }

}
