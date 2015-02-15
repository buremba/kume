package org.rakam.kume.service.ringmap;

import org.rakam.kume.transport.OperationContext;
import org.rakam.kume.transport.Request;

/**
* Created by buremba <Burak Emre Kabakcı> on 19/12/14 04:10.
*/
class GetRequest<K, V> implements Request<AbstractRingMap,V> {
    private final K key;
    private AbstractRingMap ringMap;

    public GetRequest(AbstractRingMap ringMap, K key) {
        this.key = key;
        this.ringMap = ringMap;
    }

    @Override
    public void run(AbstractRingMap service, OperationContext ctx) {
        ctx.reply(service.getPartition(ringMap.getRing().findBucketId(key)).get(key));
    }
}
