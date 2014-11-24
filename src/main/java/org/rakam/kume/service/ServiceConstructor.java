package org.rakam.kume.service;

import org.rakam.kume.Cluster;
import org.rakam.kume.service.Service;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 20:16.
 */
public interface ServiceConstructor<T extends Service> {
    public T newInstance(Cluster bus);
}
