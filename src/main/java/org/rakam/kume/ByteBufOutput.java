package org.rakam.kume;


import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;
import org.rakam.kume.util.NotImplementedException;

import java.io.OutputStream;

/**
 * A proxy serializer for Kryo to write the output into Netty buffers.
 * I'm not sure whether this is a good idea or not but if use Kryo's Output when when writing data to Netty's buffer
 * we have to clear the buffers of the Output for every object that we serialize which is an expensive operation.
 * However; this proxy class directly writes the data into Netty's buffers.
 */
public class ByteBufOutput extends Output {
    private final ByteBuf byteBuf;

    public ByteBufOutput(ByteBuf byteBuf) {
        super();
        this.byteBuf = byteBuf;
    }

    @Override
    public OutputStream getOutputStream() {
        throw new NotImplementedException();
    }

    @Override
    public void setOutputStream(OutputStream outputStream) {
        throw new NotImplementedException();
    }

    @Override
    public void setBuffer(byte[] buffer) {
        throw new NotImplementedException();
    }

    @Override
    public void setBuffer(byte[] buffer, int maxBufferSize) {
        throw new NotImplementedException();
    }

    @Override
    public byte[] getBuffer() {
        return byteBuf.array();
    }

    @Override
    public byte[] toBytes() {
        throw new NotImplementedException();
    }

    @Override
    public void setPosition(int position) {
        // netty has different positions for read and write operations.
        // so this method is not compatible.
        throw new NotImplementedException();
    }

    public void setWriterPosition(int pos) {
        byteBuf.writerIndex(pos);
    }

    public void setReaderPosition(int pos) {
        byteBuf.readerIndex(pos);
    }

    @Override
    public void clear() {
        byteBuf.clear();
    }

    @Override
    protected boolean require(int required) throws KryoException {
        return byteBuf.writableBytes() >= required;
    }

    @Override
    public void flush() throws KryoException {
        throw new NotImplementedException();
    }

    @Override
    public void close() throws KryoException {
        throw new NotImplementedException();
    }

    @Override
    public void write(int value) throws KryoException {
        byteBuf.writeByte(value);
    }

    @Override
    public void write(byte[] bytes) throws KryoException {
        byteBuf.writeBytes(bytes);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws KryoException {
        byteBuf.writeBytes(bytes, offset, length);
    }

    @Override
    public void writeByte(byte value) throws KryoException {
        byteBuf.writeByte(value);
    }

    @Override
    public void writeByte(int value) throws KryoException {
        byteBuf.writeByte(value);
    }

    @Override
    public void writeBytes(byte[] bytes) throws KryoException {
        byteBuf.writeBytes(bytes);
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int count) throws KryoException {
        byteBuf.writeBytes(bytes, offset, count);
    }

    @Override
    public void writeInt(int value) throws KryoException {
        byteBuf.writeInt(value);
    }

    @Override
    public int writeInt(int value, boolean optimizePositive) throws KryoException {
        // The ByteBuf we usually act as a proxy of unsafe.putInt, however Kryo use more sophisticated algorithm.
        // I don't know whether writing data to unsafe buffers byte by byte is better compared to unsafe.putInt so
        // we may test it in the future.
        byteBuf.writeInt(value);
        return 4;
    }

    @Override
    public int writeVarInt(int value, boolean optimizePositive) throws KryoException {
        byteBuf.writeInt(value);
        return 4;
    }

    @Override
    public void writeString(String value) throws KryoException {
        writeString((CharSequence)value);
    }


    public int capacity() {
        return byteBuf.capacity();
    }

    private void writeString_slow (CharSequence value, int charCount, int charIndex) {
        for (; charIndex < charCount; charIndex++) {
            if (position() == capacity()) require(Math.min(capacity(), charCount - charIndex));
            int c = value.charAt(charIndex);
            if (c <= 0x007F) {
                write(c);
            } else if (c > 0x07FF) {
                write((0xE0 | c >> 12 & 0x0F));
                require(2);
                write(0x80 | c >> 6 & 0x3F);
                write(0x80 | c & 0x3F);
            } else {
                write(0xC0 | c >> 6 & 0x1F);
                require(1);
                write(0x80 | c & 0x3F);
            }
        }
    }

    /** Writes the length of a string, which is a variable length encoded int except the first byte uses bit 8 to denote UTF8 and
     * bit 7 to denote if another byte is present. */
    private void writeUtf8Length (int value) {
        if (value >>> 6 == 0) {
            require(1);
            write(value | 0x80); // Set bit 8.
        } else if (value >>> 13 == 0) {
            require(2);
            write(value | 0x40 | 0x80); // Set bit 7 and 8.
            write(value >>> 6);
        } else if (value >>> 20 == 0) {
            require(3);
            write(value | 0x40 | 0x80); // Set bit 7 and 8.
            write((value >>> 6) | 0x80); // Set bit 8.
            write(value >>> 13);
        } else if (value >>> 27 == 0) {
            require(4);
            write(value | 0x40 | 0x80); // Set bit 7 and 8.
            write((value >>> 6) | 0x80); // Set bit 8.
            write((value >>> 13) | 0x80); // Set bit 8.
            write(value >>> 20);
        } else {
            require(5);
            write(value | 0x40 | 0x80); // Set bit 7 and 8.
            write((value >>> 6) | 0x80); // Set bit 8.
            write((value >>> 13) | 0x80); // Set bit 8.
            write((value >>> 20) | 0x80); // Set bit 8.
            write(value >>> 27);
        }
    }

    @Override
    public void writeString(CharSequence value) throws KryoException {
        if (value == null) {
            writeByte(0x80); // 0 means null, bit 8 means UTF8.
            return;
        }
        int charCount = value.length();
        if (charCount == 0) {
            writeByte(1 | 0x80); // 1 means empty string, bit 8 means UTF8.
            return;
        }
        // TODO: For now, treat all strings as UTF.
        // Detect ASCII.
//        boolean ascii = false;
//        if (charCount > 1 && charCount < 64) {
//            ascii = true;
//            for (int i = 0; i < charCount; i++) {
//                int c = value.charAt(i);
//                if (c > 127) {
//                    ascii = false;
//                    break;
//                }
//            }
//        }
//        if (ascii) {
//            if (capacity() - position() < charCount)
//                writeAscii_slow(value, charCount);
//            else {
//                value.getBytes(0, charCount, buffer, position);
//            }
//            buffer[position() - 1] |= 0x80;
//        } else {
        writeUtf8Length(charCount + 1);
        int charIndex = 0;
        if (capacity() - position() >= charCount) {
            // Try to write 8 bit chars.
            for (; charIndex < charCount; charIndex++) {
                int c = value.charAt(charIndex);
                if (c > 127) break;
                write(c);
            }
        }
        if (charIndex < charCount) writeString_slow(value, charCount, charIndex);
//        }
    }

    @Override
    public void writeAscii(String value) throws KryoException {
        writeString((CharSequence)value);
    }

    @Override
    public void writeFloat(float value) throws KryoException {
        byteBuf.writeFloat(value);
    }

    @Override
    public int writeFloat(float value, float precision, boolean optimizePositive) throws KryoException {
        byteBuf.writeFloat(value);
        return 4;
    }

    @Override
    public void writeShort(int value) throws KryoException {
        byteBuf.writeShort(value);
    }

    @Override
    public void writeLong(long value) throws KryoException {
        byteBuf.writeLong(value);
    }

    @Override
    public int writeLong(long value, boolean optimizePositive) throws KryoException {
        byteBuf.writeLong(value);
        return 8;
    }

    @Override
    public int writeVarLong(long value, boolean optimizePositive) throws KryoException {
        byteBuf.writeLong(value);
        return 8;
    }

    @Override
    public void writeBoolean(boolean value) throws KryoException {
        byteBuf.writeBoolean(value);
    }

    @Override
    public void writeChar(char value) throws KryoException {
        byteBuf.writeChar(value);
    }

    @Override
    public void writeDouble(double value) throws KryoException {
        byteBuf.writeDouble(value);
    }

    @Override
    public int writeDouble(double value, double precision, boolean optimizePositive) throws KryoException {
        byteBuf.writeDouble(value);
        return 8;
    }

}
