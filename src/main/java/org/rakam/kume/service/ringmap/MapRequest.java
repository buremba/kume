package org.rakam.kume.service.ringmap;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.rakam.kume.Request;

import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/11/14 18:47.
 */
abstract class MapRequest<T> implements Request<T> {
    @FieldSerializer.Optional("map")
    protected Map<String, Integer> map;

    @Override
    public int getService() {
        return RingMap.SERVICE_ID;
    }

    public Map<String, Integer> getMap() {
        return map;
    }

    public void setMap(Map<String, Integer> map) {
        this.map = map;
    }
}
