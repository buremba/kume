package org.rakam.kume.service.ringmap;

import org.rakam.kume.transport.Request;


public interface PartitionRestrictedMapRequest<C extends AbstractRingMap, V> extends Request<C,V>
{
    public int getPartition(AbstractRingMap service);
}
