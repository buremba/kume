package org.rakam.kume;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/01/15 06:01.
 */
public class ByteBufInput extends Input {
    private final ByteBuf byteBuf;

    public ByteBufInput(ByteBuf byteBuf) {
        super();
        this.byteBuf = byteBuf;
    }

    @Override
    public void setBuffer(byte[] bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBuffer(byte[] bytes, int offset, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBuffer() {
        return byteBuf.array();
    }

    @Override
    public InputStream getInputStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setInputStream(InputStream inputStream) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long total() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTotal(long total) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPosition(int position) {
        byteBuf.readerIndex(position);
    }

    @Override
    public void setLimit(int limit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rewind() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skip(int count) throws KryoException {
        byteBuf.skipBytes(count);
    }

    @Override
    protected int fill(byte[] buffer, int offset, int count) throws KryoException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int require(int required) throws KryoException {
        return byteBuf.readerIndex();
    }

    @Override
    public boolean eof() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int available() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read() throws KryoException {
        return byteBuf.readByte();
    }

    @Override
    public int read(byte[] bytes) throws KryoException {
        byteBuf.readBytes(bytes);
        return bytes.length;
    }

    @Override
    public int read(byte[] bytes, int offset, int count) throws KryoException {
        byteBuf.readBytes(bytes, offset, count);
        return bytes.length;
    }

    @Override
    public long skip(long count) throws KryoException {
        byteBuf.skipBytes((int) count);
        return byteBuf.readerIndex();
    }

    @Override
    public void close() throws KryoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte() throws KryoException {
        return byteBuf.readByte();
    }

    @Override
    public int readByteUnsigned() throws KryoException {
        return byteBuf.readUnsignedByte();
    }

    @Override
    public byte[] readBytes(int length) throws KryoException {
        return byteBuf.readBytes(length).array();
    }

    @Override
    public void readBytes(byte[] bytes) throws KryoException {
        byteBuf.readBytes(bytes);
    }

    @Override
    public void readBytes(byte[] bytes, int offset, int count) throws KryoException {
        byteBuf.readBytes(bytes, offset, count);
    }

    @Override
    public int readInt() throws KryoException {
        return byteBuf.readInt();
    }

    @Override
    public int readInt(boolean optimizePositive) throws KryoException {
        return byteBuf.readInt();
    }

    @Override
    public int readVarInt(boolean optimizePositive) throws KryoException {
        return byteBuf.readInt();
    }

    @Override
    public boolean canReadInt() throws KryoException {
        return byteBuf.readableBytes() >= 4;
    }

    @Override
    public boolean canReadLong() throws KryoException {
        return byteBuf.readableBytes() >= 8;
    }

    @Override
    public String readString() {
        int available = require(1);
        int b = byteBuf.readByte();
//        if ((b & 0x80) == 0) return readAscii(); // ASCII.
//        Null, empty, or UTF8.
        int charCount = available >= 5 ? readUtf8Length(b) : readUtf8Length_slow(b);
        switch (charCount) {
            case 0:
                return null;
            case 1:
                return "";
        }
        charCount--;
        if (chars.length < charCount) chars = new char[charCount];
        readUtf8(charCount);
        return new String(chars, 0, charCount);
    }

    private int readUtf8Length (int b) {
        int result = b & 0x3F; // Mask all but first 6 bits.
        if ((b & 0x40) != 0) { // Bit 7 means another byte, bit 8 means UTF8.
            b = byteBuf.readByte();
            result |= (b & 0x7F) << 6;
            if ((b & 0x80) != 0) {
                b = byteBuf.readByte();
                result |= (b & 0x7F) << 13;
                if ((b & 0x80) != 0) {
                    b = byteBuf.readByte();
                    result |= (b & 0x7F) << 20;
                    if ((b & 0x80) != 0) {
                        b = byteBuf.readByte();
                        result |= (b & 0x7F) << 27;
                    }
                }
            }
        }
        return result;
    }

    private void readUtf8 (int charCount) {
        char[] chars = this.chars;
        // Try to read 7 bit ASCII chars.
        int charIndex = 0;
        int count = Math.min(require(1), charCount);
        int b;
        while (charIndex < count) {
            b = byteBuf.readByte();
            if (b < 0) {
                byteBuf.readerIndex(byteBuf.readerIndex()-1);
                break;
            }
            chars[charIndex++] = (char)b;
        }
        // If buffer didn't hold all chars or any were not ASCII, use slow path for remainder.
        if (charIndex < charCount) readUtf8_slow(charCount, charIndex);
    }

    private void readUtf8_slow (int charCount, int charIndex) {
        char[] chars = this.chars;
        while (charIndex < charCount) {
            if (byteBuf.readableBytes() == 0) require(1);
            int b = byteBuf.readByte() & 0xFF;
            switch (b >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    chars[charIndex] = (char)b;
                    break;
                case 12:
                case 13:
                    if (byteBuf.readableBytes() == 0) require(1);
                    chars[charIndex] = (char)((b & 0x1F) << 6 | byteBuf.readByte() & 0x3F);
                    break;
                case 14:
                    require(2);
                    chars[charIndex] = (char)((b & 0x0F) << 12 | (byteBuf.readByte() & 0x3F) << 6 | byteBuf.readByte() & 0x3F);
                    break;
            }
            charIndex++;
        }
    }

    private int readUtf8Length_slow (int b) {
        int result = b & 0x3F; // Mask all but first 6 bits.
        if ((b & 0x40) != 0) { // Bit 7 means another byte, bit 8 means UTF8.
            require(1);
            b = byteBuf.readByte();
            result |= (b & 0x7F) << 6;
            if ((b & 0x80) != 0) {
                require(1);
                b = byteBuf.readByte();
                result |= (b & 0x7F) << 13;
                if ((b & 0x80) != 0) {
                    require(1);
                    b = byteBuf.readByte();
                    result |= (b & 0x7F) << 20;
                    if ((b & 0x80) != 0) {
                        require(1);
                        b = byteBuf.readByte();
                        result |= (b & 0x7F) << 27;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public StringBuilder readStringBuilder() {
        int available = require(1);
        int b = byteBuf.readByte();
//        if ((b & 0x80) == 0) return new StringBuilder(readAscii()); // ASCII.
        // Null, empty, or UTF8.
        int charCount = available >= 5 ? readUtf8Length(b) : readUtf8Length_slow(b);
        switch (charCount) {
            case 0:
                return null;
            case 1:
                return new StringBuilder("");
        }
        charCount--;
        if (chars.length < charCount) chars = new char[charCount];
        readUtf8(charCount);
        StringBuilder builder = new StringBuilder(charCount);
        builder.append(chars, 0, charCount);
        return builder;
    }

    @Override
    public float readFloat() throws KryoException {
        return byteBuf.readFloat();
    }

    @Override
    public float readFloat(float precision, boolean optimizePositive) throws KryoException {
        return byteBuf.readFloat();
    }

    @Override
    public short readShort() throws KryoException {
        return byteBuf.readShort();
    }

    @Override
    public int readShortUnsigned() throws KryoException {
        return byteBuf.readUnsignedShort();
    }

    @Override
    public long readLong() throws KryoException {
        return byteBuf.readLong();
    }

    @Override
    public long readLong(boolean optimizePositive) throws KryoException {
        return byteBuf.readLong();
    }

    @Override
    public long readVarLong(boolean optimizePositive) throws KryoException {
        return byteBuf.readLong();
    }

    @Override
    public boolean readBoolean() throws KryoException {
        return byteBuf.readBoolean();
    }

    @Override
    public char readChar() throws KryoException {
        return byteBuf.readChar();
    }

    @Override
    public double readDouble() throws KryoException {
        return byteBuf.readDouble();
    }

    @Override
    public double readDouble(double precision, boolean optimizePositive) throws KryoException {
        return byteBuf.readDouble();
    }

}
