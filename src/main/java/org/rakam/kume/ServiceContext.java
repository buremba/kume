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

import io.netty.channel.nio.NioEventLoopGroup;
import org.rakam.kume.service.Service;
import org.rakam.kume.transport.Request;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/15 01:55.
 */
public class ServiceContext<T extends Service> {
    private final String serviceName;
    private final int service;
    private final Cluster cluster;

    public ServiceContext(Cluster cluster, int service, String serviceName) {
        String serviceName1;
        this.service = service;
        serviceName1 = serviceName;
        this.serviceName = serviceName1;
        this.cluster = cluster;
    }

    public void send(Member server, Object bytes) {
        cluster.sendInternal(server, bytes, service);
    }

    public <R> void send(Member server, Request<T, R> request) {
        cluster.sendInternal(server, request, service);
    }

    public void sendAllMembers(Object bytes) {
        sendAllMembers(bytes, false);
    }

    public <R> void sendAllMembers(Request<T, R> bytes) {
        sendAllMembers(bytes, false);
    }

    public <R> void sendAllMembers(Object bytes, boolean includeThisMember) {
        cluster.sendAllMembersInternal(bytes, includeThisMember, service);
    }

    public <R> void sendAllMembers(Request<T, R> bytes, boolean includeThisMember) {
        cluster.sendAllMembersInternal(bytes, includeThisMember, service);
    }

    public int serviceId() {
        return service;
    }

    public String serviceName() {
        return serviceName;
    }

    public <R> CompletableFuture<R> ask(Member server, Object bytes) {
        return cluster.askInternal(server, bytes, service);
    }

    public <R> CompletableFuture<R> ask(Member server, Request<T, R> request) {
        return cluster.askInternal(server, request, service);
    }

    public <R> CompletableFuture<R> ask(Member server, Request<T, R> request, Class<R> clazz) {
        return ask(server, request);
    }

    public <R> Map<Member, CompletableFuture<R>> askAllMembers(Object bytes) {
        return cluster.askAllMembersInternal(bytes, false, service);
    }

    public <R> Map<Member, CompletableFuture<R>> askAllMembers(Request<T, R> bytes, boolean includeThisMember) {
        return cluster.askAllMembersInternal(bytes, includeThisMember, service);
    }

    public <R> Map<Member, CompletableFuture<R>> askAllMembers(Object bytes, boolean includeThisMember) {
        return cluster.askAllMembersInternal(bytes, includeThisMember, service);
    }

    public <R> Map<Member, CompletableFuture<R>> askAllMembers(Request<T, R> bytes) {
        return askAllMembers(bytes, true);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public <R> CompletableFuture<R> tryAskUntilDone(Member member, Request<T, R> req, int numberOfTimes) {
        CompletableFuture<R> f = new CompletableFuture<>();
        cluster.tryAskUntilDoneInternal(member, req, numberOfTimes, service, f);
        return f;
    }

    public CompletableFuture<Boolean> replicateSafely(Request<T, Boolean> req) {
        return cluster.replicateSafelyInternal(req, service);
    }

    public <R> CompletableFuture<R> tryAskUntilDone(Member member, Request<T, R> req, int numberOfTimes, Class<R> clazz) {
        return tryAskUntilDone(member, req, numberOfTimes);
    }

    public NioEventLoopGroup eventLoop() {
        return cluster.eventLoop;
    }
}
