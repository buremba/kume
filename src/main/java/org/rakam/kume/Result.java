package org.rakam.kume;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/11/14 19:12.
 */
public class Result<T> {
    Object data;
    final boolean success;

    protected static Result FAILED = new Result(false);

    public Result(boolean success, Object data) {
        this.data = data;
        this.success = success;
    }

    public T getData() {
        return (T) data;
    }

    public Result(boolean success) {
        this.success = success;
    }

    public boolean isSucceeded() {
        return success;
    }

    public boolean isFailed() {
        return !success;
    }
}
