package org.rakam.kume.transport.serialization;

import com.google.common.hash.PrimitiveSink;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/01/15 06:01.
 */
public interface SinkSerializable {
    public void writeTo(PrimitiveSink sink);
}
