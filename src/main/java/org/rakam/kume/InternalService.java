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

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/15 01:57.
 */
public class InternalService extends Service
{
    protected final Cluster cluster;
    private final ServiceContext<InternalService> ctx;

    InternalService(ServiceContext<InternalService> ctx, Cluster cluster) {
        this.ctx = ctx;
        this.cluster = cluster;
    }

    public ServiceContext<InternalService> getContext() {
        return ctx;
    }

    @Override
    public void onClose() {

    }
}
