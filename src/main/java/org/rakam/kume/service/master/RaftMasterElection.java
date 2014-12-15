package org.rakam.kume.service.master;

import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.OperationContext;
import org.rakam.kume.Result;
import org.rakam.kume.service.Service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 00:08.
 */
public class RaftMasterElection implements Service {
    Cluster.ServiceContext bus;

    public RaftMasterElection(Cluster.ServiceContext cluster) {
        this.bus = cluster;
    }

    public void voteElection() {
        Collection<Member> clusterMembers = bus.getCluster().getMembers();

        Map<String, Boolean> map = new ConcurrentHashMap<>();
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        Map<Member, CompletionStage<Result>> m = bus.askAllMembers((service, ctx) -> ctx.reply(true));
        m.forEach((member, resultFuture) -> {
            resultFuture.thenAccept(result -> {
                System.out.println(result.getData());
                if (result.isSucceeded()) {
                    map.put("a", (Boolean) result.getData());
                } else {
                    System.out.println("why");
                }

                Map<Boolean, Long> stream = map.entrySet().stream()
                        .collect(Collectors.groupingBy(o -> o.getValue(), Collectors.counting()));
                if (stream.getOrDefault(Boolean.TRUE, 0L) > clusterMembers.size() / 2) {
                    future.complete(true);
                } else if (stream.getOrDefault(Boolean.FALSE, 0L) > clusterMembers.size() / 2) {
                    future.complete(false);
                }
            });
        });
    }



    @Override
    public void handle(OperationContext ctx, Object request) {

    }
}
