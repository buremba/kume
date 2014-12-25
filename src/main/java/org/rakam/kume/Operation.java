package org.rakam.kume;

import org.rakam.kume.service.Service;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/12/14 16:51.
 */
public interface Operation<T extends Service> extends Request<T, Void> {
}
