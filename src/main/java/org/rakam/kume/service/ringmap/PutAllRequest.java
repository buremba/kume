package org.rakam.kume.service.ringmap;

import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 18/12/14 14:58.
 */
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
