package org.rakam.kume.transport.serialization;

import com.google.common.hash.PrimitiveSink;


public interface SinkSerializable {
    public void writeTo(PrimitiveSink sink);
}
