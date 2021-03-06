package org.rakam.kume;

import com.esotericsoftware.kryo.NotNull;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.rakam.kume.transport.serialization.serializers.InetSocketAddressSerializer;

import java.net.InetSocketAddress;


public class Member {
    @NotNull
    @FieldSerializer.Bind(InetSocketAddressSerializer.class)
    private final InetSocketAddress address;
    private final boolean client;

    public InetSocketAddress getAddress() {
        return address;
    }

    public boolean isClient() {
        return client;
    }

    public Member(InetSocketAddress address) {
        this.address = address;
        client = false;
    }

    public Member(InetSocketAddress address, boolean client) {
        this.address = address;
        this.client = client;
    }

    @Override
    public String toString() {
        return "Member{" +
                "address=" + address +
                ", client=" + client +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Member)) return false;

        Member member = (Member) o;

        if (!address.equals(member.address)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    public Member(String host, int port) {
        address = new InetSocketAddress(host, port);
        client = false;
    }
}
