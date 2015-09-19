package org.rakam.kume.service;


import org.rakam.kume.ServiceContext;

import java.io.Serializable;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/11/14 20:16.
 */
@FunctionalInterface
public interface ServiceConstructor<T extends Service> extends Serializable {
    public T newInstance(ServiceContext bus);
}
