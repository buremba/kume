package org.rakam.kume.service;


import org.rakam.kume.ServiceContext;

import java.io.Serializable;


@FunctionalInterface
public interface ServiceConstructor<T extends Service> extends Serializable {
    T newInstance(ServiceContext bus);
}
