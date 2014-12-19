package org.rakam.kume.service.ringmap;

import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 18/12/14 14:58.
 */
public class PutAllMapOperation implements Request<RingMap> {

    private final List<Map.Entry<String, Integer>> entries;

    public PutAllMapOperation(List<Map.Entry<String, Integer>> entries) {
        this.entries = entries;
    }

    @Override
    public void run(RingMap service, OperationContext ctx) {
        for (Map.Entry<String, Integer> entry : entries) {
            service.putLocal(entry.getKey(), entry.getValue());
        }
    }
}
