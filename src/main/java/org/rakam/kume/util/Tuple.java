package org.rakam.kume.util;


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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tuple)) return false;

        Tuple tuple = (Tuple) o;

        if (_1 != null ? !_1.equals(tuple._1) : tuple._1 != null) return false;
        if (_2 != null ? !_2.equals(tuple._2) : tuple._2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _1 != null ? _1.hashCode() : 0;
        result = 31 * result + (_2 != null ? _2.hashCode() : 0);
        return result;
    }
}
