package org.rakam.kume.service;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;


public class ServiceListBuilder {
    List<Constructor> constructorList;

    public ServiceListBuilder() {
        this.constructorList = new ArrayList<>();
    }

    public synchronized ServiceListBuilder add(String name, ServiceConstructor constructor) {
        constructorList.add(new Constructor(name, constructor));
        return this;
    }

    public ImmutableList<Constructor> build() {
        return ImmutableList.copyOf(constructorList);
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
