package org.rakam.kume;

import com.esotericsoftware.kryo.Kryo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.rakam.kume.transport.serialization.KryoFactory;
import org.rakam.kume.util.Tuple;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/01/15 05:50.
 */
public class SerializationTest {

    @Test
    public void testByteBuf() throws InterruptedException {

        Tuple<String, Integer> stringIntegerTuple = new Tuple<>("333", 4);

        Kryo kryoInstance = KryoFactory.getKryoInstance();

        ByteBuf buffer = Unpooled.buffer(2 << 10);
        ByteBufOutput output = new ByteBufOutput(buffer);
//        Output output = new Output(2 << 10);
        kryoInstance.writeClassAndObject(output, stringIntegerTuple);
        output.write(5);

//        output.setWriterPosition(0);
        Object o = kryoInstance.readClassAndObject(new ByteBufInput(buffer));
        System.out.println(o);

    }
}
