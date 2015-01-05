package org.rakam.kume.service.ringmap;

import org.rakam.kume.transport.Request;

/**
 * Created by buremba <Burak Emre Kabakcı> on 05/01/15 20:35.
 */
public interface PartitionRestrictedMapRequest<C extends AbstractRingMap, V> extends Request<C,V> {
    public int getPartition(AbstractRingMap service);
}
