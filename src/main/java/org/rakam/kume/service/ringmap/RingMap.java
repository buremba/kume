package org.rakam.kume.service.ringmap;

import org.rakam.kume.transport.OperationContext;
import org.rakam.kume.util.ConsistentHashRing;
import io.netty.util.concurrent.EventExecutor;
import org.rakam.kume.Member;
import org.rakam.kume.ServiceContext;
import org.rakam.kume.transport.Request;
import org.rakam.kume.util.FutureUtil;
import org.rakam.kume.util.ThrowableNioEventLoopGroup;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.rakam.kume.util.ConsistentHashRing.hash;

public class RingMap<K, V> extends AbstractRingMap<RingMap, Map, K, V> {
    private final static int DEFAULT_BUCKET_COUNT = 8;

    public RingMap(ServiceContext<RingMap> serviceContext, Supplier<Map> mapSupplier, MapMergePolicy<V> mergePolicy, int bucketCount, int replicationFactor) {
        super(serviceContext, mapSupplier, mergePolicy, bucketCount, replicationFactor);
    }

    public RingMap(ServiceContext<RingMap> serviceContext, MapMergePolicy<V> mergePolicy, int replicationFactor) {
        super(serviceContext, ConcurrentHashMap::new, mergePolicy, DEFAULT_BUCKET_COUNT, replicationFactor);
    }

    public CompletableFuture<V> merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        int bucketId = getRing().findBucketIdFromToken(ConsistentHashRing.hash(key));
        ConsistentHashRing.Bucket bucket = getRing().getBucket(bucketId);

        FutureUtil.MultipleFutureListener listener = new FutureUtil.MultipleFutureListener((bucket.members.size() / 2) + 1);
        for (Member next : bucket.members) {
            listener.listen(getContext().ask(next, new MergeMapOperation(key, value, remappingFunction)));
        }

        return listener.get();
    }

    protected V mergeLocal(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        Map<K, V> partition = getBucket(getRing().findBucketId(key));
        V oldValue = partition.get(key);
        V newValue = (oldValue == null) ? value : remappingFunction.apply(oldValue, value);
        if (newValue == null) {
            partition.remove(key);
            return null;
        } else {
            partition.put(key, newValue);
            return newValue;
        }
    }

    /**
     * Since we use ConcurrentHashMap, most of the operation may run concurrently.
     * However
     *
     * @param executor
     * @param ctx
     * @param request
     */
    @Override
    public void handle(ThrowableNioEventLoopGroup executor, OperationContext ctx, Request request) {
        if (request instanceof PartitionRestrictedMapRequest) {
            int id = ((PartitionRestrictedMapRequest) request).getPartition(this) % executor.executorCount();
            EventExecutor child = executor.getChild(id);
            if (child.inEventLoop()) {
                try {
                    request.run(this, ctx);
                } catch (Exception e) {
                    LOGGER.error("error while running throwable code block", e);
                }
            } else {
                child.execute(() -> request.run(this, ctx));
            }
        } else {
            executor.execute(() -> request.run(this, ctx));
        }
    }


    public static class MergeMapOperation<V> implements PartitionRestrictedMapRequest<RingMap, V> {
        private final BiFunction remappingFunction;
        Object key;
        Object value;

        public MergeMapOperation(Object key, Object value, BiFunction remappingFunction) {
            this.key = key;
            this.value = value;
            this.remappingFunction = remappingFunction;
        }

        @Override
        public void run(RingMap service, OperationContext ctx) {
            ctx.reply(service.mergeLocal(key, value, remappingFunction));
        }

        @Override
        public int getPartition(AbstractRingMap service) {
            return service.getPartitionId(service.getRing().findBucketId(key));
        }
    }
}