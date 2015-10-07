package org.rakam.kume.transport;

import org.rakam.kume.service.Service;


public interface Operation<T extends Service> extends Request<T, Void> {
}
