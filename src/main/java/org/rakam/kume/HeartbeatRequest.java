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

import org.rakam.kume.transport.Operation;
import org.rakam.kume.transport.OperationContext;
import com.google.auto.service.AutoService;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/15 01:55.
 */
@KryoSerializable(id = 3)
@AutoService(KryoSerializable.class)
public class HeartbeatRequest implements Operation<InternalService>
{
    Member sender;

    public HeartbeatRequest(Member sender) {
        this.sender = sender;
    }

    @Override
    public void run(InternalService service, OperationContext ctx) {
        Member masterMember = service.cluster.getMaster();
        if (sender == null) {
            return;
        }
        if (sender.equals(masterMember)) {
            service.cluster.lastContactedTimeMaster = System.currentTimeMillis();
        } else {
            Cluster.LOGGER.trace("got message from a member who thinks he is the master: {0}", sender);
            if (!service.cluster.getMembers().contains(sender)) {
                Cluster.LOGGER.trace("it seems this is new master added me in his cluster and" +
                        " it will most probably send changeCluster request to me");
            }
        }
    }
}
