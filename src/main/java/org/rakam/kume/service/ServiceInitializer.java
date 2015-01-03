package org.rakam.kume.service;

import java.util.ArrayList;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/11/14 15:52.
 */
public class ServiceInitializer extends ArrayList<ServiceInitializer.Constructor> {

    public ServiceInitializer add(String name, ServiceConstructor constructor) {
        add(new Constructor(name, constructor));
        return this;
    }

    public static class Constructor {
        public final String name;
        public final ServiceConstructor constructor;

        public Constructor(String name, ServiceConstructor constructor) {
            this.constructor = constructor;
            this.name = name;
        }
    }
}
