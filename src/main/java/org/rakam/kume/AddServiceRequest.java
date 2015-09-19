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
import org.rakam.kume.service.ServiceConstructor;
import org.rakam.kume.transport.OperationContext;
import org.rakam.kume.transport.Request;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/15 01:51.
 */
public class AddServiceRequest implements Request<InternalService, Boolean> {
    String finalName;
    String name;
    ServiceConstructor constructor;

    public AddServiceRequest(String finalName, String name, ServiceConstructor constructor) {
        this.finalName = finalName;
        this.name = name;
        this.constructor = constructor;
    }

    @Override
    public void run(InternalService service, OperationContext<Boolean> ctx) {
        Cluster cluster = service.cluster;

        if (cluster.getService(finalName) != null) {
            ctx.reply(false);
        }


        Service s = constructor.newInstance(new ServiceContext(service.cluster, cluster.getServices().size(), name));
        // service variable is not thread-safe
        synchronized (cluster.getServices()) {
            cluster.getServices().add(s);
        }
        cluster.serviceNameMap.put(finalName, s);
        ctx.reply(true);
    }
}
