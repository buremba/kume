package org.rakam.kume;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.reflect.ClassPath;
import com.pholser.junit.quickcheck.ForAll;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.rakam.kume.transport.serialization.KryoFactory;
import org.rakam.kume.transport.Request;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/01/15 05:50.
 */
//@RunWith(Theories.class)
public class SerializationTest {

//    @Theory
    public void testByteBuf(@ForAll Request obj) throws InterruptedException {
        Kryo kryoInstance = KryoFactory.getKryoInstance();

        ByteBuf buffer = Unpooled.buffer(2 << 10);
        ByteBufOutput output = new ByteBufOutput(buffer);
        kryoInstance.writeClassAndObject(output, obj);

        Object o = kryoInstance.readClassAndObject(new ByteBufInput(buffer));
        assertEquals(o, obj);
    }

    /*
        Scan all subclasses of Request and test if they're serializable or not.
        Since we cannot scan local variables using reflection api, lambdas and anonymous classes will not be in this scope.
        TODO: Add registered lambdas and anonymous classes via kryo factory.
     */
//    @Test
    public void tesstByteBuf() throws InterruptedException, IOException {
        ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        ClassPath from = ClassPath.from(systemClassLoader);
        StdInstantiatorStrategy instantiator = new StdInstantiatorStrategy();

        Class<Request> requestClazz = Request.class;
        for (ClassPath.ClassInfo clazz : from.getAllClasses()) {
            Class<?> load;
            try {
                load = clazz.load();
            } catch (NoClassDefFoundError e) {
                continue;
            }

            for (Class<?> aClass : load.getInterfaces()) {
                if (aClass.equals(requestClazz)) {
                    Object o = instantiator.newInstantiatorOf(aClass).newInstance();
                    testByteBuf((Request) o);
                }
            }
        }
    }

}
