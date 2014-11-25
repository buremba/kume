package org.rakam.kume;

import org.rakam.kume.service.ServiceConstructor;

import java.util.ArrayList;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/11/14 15:52.
 */
public class ServiceInitializer extends ArrayList<ServiceInitializer.Constructor> {

    public ServiceInitializer add(ServiceConstructor constructor, int numberOfInstance) {
        add(new Constructor(constructor, numberOfInstance));
        return this;
    }

    public ServiceInitializer add(ServiceConstructor constructor) {
        return add(constructor, 1);
    }

    public static class Constructor {
        ServiceConstructor constructor;
        int numberOfInstance;

        public Constructor(ServiceConstructor constructor, int numberOfInstance) {
            this.constructor = constructor;
            this.numberOfInstance = numberOfInstance;
        }
    }
}
