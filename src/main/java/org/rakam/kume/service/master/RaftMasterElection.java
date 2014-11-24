package org.rakam.kume.service.master;

import org.rakam.kume.Cluster;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/11/14 00:08.
 */
public class RaftMasterElection {
    Cluster cluster;

    public RaftMasterElection(Cluster cluster) {
        this.cluster = cluster;
    }

    public CompletableFuture<Boolean> voteElection() {
        Map<String, Boolean> map = new ConcurrentHashMap<>();
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        cluster.getClusterMembers().forEach((node) -> {
            cluster.send(node.getId(), (ElectionRequest<Boolean>) () -> true).thenAccept(result -> {
                System.out.println(result.getData());
                if (result.isSucceeded()) {
                    map.put(node.getId(), (Boolean) result.getData());
                } else {
                    System.out.println("why");
                }

                Map<Boolean, Long> stream = map.entrySet().stream()
                        .collect(Collectors.groupingBy(o -> o.getValue(), Collectors.counting()));
                if (stream.getOrDefault(Boolean.TRUE, 0L) > cluster.getClusterMembers().size() / 2) {
                    future.complete(true);
                } else if (stream.getOrDefault(Boolean.FALSE, 0L) > cluster.getClusterMembers().size() / 2) {
                    future.complete(false);
                }
            });
        });
        return future;
    }


}
