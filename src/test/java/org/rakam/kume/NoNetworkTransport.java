/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.kume;

import org.rakam.kume.service.Service;
import org.rakam.kume.transport.LocalOperationContext;
import org.rakam.kume.transport.Packet;
import org.rakam.kume.transport.Request;
import org.rakam.kume.util.ThrowableNioEventLoopGroup;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * Instead of sending packets using network connections, it simply find the local instance and send it directly to the related service.
 * It works only for Kume instances created on same JVM. Since it avoids network operations, it's mostly used for testing purposes.
 */
public class NoNetworkTransport implements Transport {
    private final Map<Member, NoNetworkTransport> bus;
    private ThrowableNioEventLoopGroup requestExecutor;
    private List<Service> services;
    private Member localMember;
    private Map<Member, MemberChannel> channels;

    public NoNetworkTransport(Member localMember) {
        this.localMember = localMember;
        bus = new ConcurrentHashMap<>();
        bus.put(localMember, this);
    }

    public NoNetworkTransport setContext(ThrowableNioEventLoopGroup requestExecutor, List<Service> services, Member localMember) {
        checkState(localMember.equals(this.localMember), "localMember doesn't match");
        this.requestExecutor = requestExecutor;
        this.services = services;
        this.localMember = localMember;
        return this;
    }

    public NoNetworkTransport addMember(NoNetworkTransport transport) {
        bus.put(transport.localMember, transport);
        return this;
    }

    public Member getLocalMember() {
        return localMember;
    }

    @Override
    public synchronized MemberChannel connect(Member member) throws InterruptedException {
        if (channels == null) {
            throw new IllegalStateException();
        }
        return channels.compute(member, (key, value) -> new NoNetworkChannel(member));
    }

    @Override
    public void close() {
        channels.clear();
    }

    @Override
    public synchronized void initialize() {
        channels = new ConcurrentHashMap<>();
    }

    public class NoNetworkChannel implements MemberChannel {
        private final Member member;

        public NoNetworkChannel(Member member) {
            this.member = member;
        }

        @Override
        public CompletableFuture ask(Packet message) {
            CompletableFuture future = new CompletableFuture<>();
            LocalOperationContext ctx1 = new LocalOperationContext(future, message.service, localMember);
            if (message.data instanceof Request) {
                if(!(message.data instanceof HeartbeatRequest)) {
                    int i = 0;
                }
                bus.get(member).services.get(message.service).handle(requestExecutor, ctx1, (Request) message.data);
            } else {
                bus.get(member).services.get(message.service).handle(requestExecutor, ctx1, message.data);
            }
            return future;
        }

        @Override
        public void send(Packet message) {
            LocalOperationContext ctx1 = new LocalOperationContext(null, message.service, localMember);
            if (message.data instanceof Request) {
                services.get(message.service).handle(requestExecutor, ctx1, (Request) message.data);
            } else {
                services.get(message.service).handle(requestExecutor, ctx1, message.data);
            }
        }

        @Override
        public void close() throws InterruptedException {
            channels.remove(this);
        }
    }
}
