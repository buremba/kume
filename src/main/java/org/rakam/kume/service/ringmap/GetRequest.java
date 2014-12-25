package org.rakam.kume.service.ringmap;

import org.rakam.kume.OperationContext;
import org.rakam.kume.Request;

/**
* Created by buremba <Burak Emre Kabakcı> on 19/12/14 04:10.
*/
class GetRequest<V> implements Request<RingMap, V> {
    private RingMap ringMap;
    private final String key;

    public GetRequest(RingMap ringMap, String key) {
        this.ringMap = ringMap;
        this.key = key;
    }

    @Override
    public void run(RingMap service, OperationContext ctx) {
        ctx.reply(service.getPartition(ringMap.getRing().findBucketId(key)).get(key));
    }
}
