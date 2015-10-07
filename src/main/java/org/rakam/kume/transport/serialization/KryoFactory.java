package org.rakam.kume.transport.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import org.rakam.kume.transport.serialization.serializers.InetSocketAddressSerializer;
import org.rakam.kume.transport.serialization.serializers.UnmodifiableCollectionsSerializer;
import org.rakam.kume.util.ConsistentHashRing;
import com.google.common.collect.ImmutableMap;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.rakam.kume.HeartbeatRequest;
import org.rakam.kume.Member;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;


public class KryoFactory {
    static {
//        Log.TRACE = true;
//        Log.DEBUG = true;
    }

    private static final Class<?>[] REG_CLASSES = {
            Collections.unmodifiableList(new ArrayList()).getClass(),
            HeartbeatRequest.class,
            Member.class,
            InetSocketAddress.class,
            ConsistentHashRing.class,
            ConsistentHashRing.Bucket.class,
    };

    private static final Map<Class, Serializer> SERIALIZERS = ImmutableMap.of(
            InetSocketAddress.class, new InetSocketAddressSerializer(),
            Collections.unmodifiableList(new ArrayList()).getClass(), new UnmodifiableCollectionsSerializer(),
            Collections.unmodifiableSet(new HashSet<>()).getClass(), new UnmodifiableCollectionsSerializer()
    );

    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            UnmodifiableCollectionsSerializer.registerSerializers(kryo);
            for (Class<?> clazz : REG_CLASSES) {
                Serializer serializer = SERIALIZERS.get(clazz);
                if (serializer == null)
                    kryo.register(clazz);
                else
                    kryo.register(clazz, serializer);
            }
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            return kryo;
        }
    };


    public static Kryo getKryoInstance() {
        return kryos.get();
    }
}
