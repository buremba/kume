package org.rakam.kume.service.ringmap;

import org.rakam.kume.transport.OperationContext;
import org.rakam.kume.transport.Request;

import java.util.List;
import java.util.Map;


public class PutAllRequest implements Request<RingMap, Void> {

    private final List<Map.Entry> entries;

    public PutAllRequest(List<Map.Entry> entries) {
        this.entries = entries;
    }

    @Override
    public void run(RingMap service, OperationContext ctx) {
        for (Map.Entry<Object, Object> entry : entries) {
            service.putLocal(entry.getKey(), entry.getValue());
        }
    }
}
