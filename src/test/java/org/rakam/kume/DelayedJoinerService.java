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

import java.time.Duration;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


class DelayedJoinerService implements JoinerService {
    private final Duration duration;
    private final ArrayBlockingQueue<Member> members;
    private final ScheduledExecutorService executor;


    public DelayedJoinerService(List<Member> addedMembers, Duration duration) {
        members = new ArrayBlockingQueue<>(addedMembers.size());
        members.addAll(addedMembers);
        executor = Executors.newSingleThreadScheduledExecutor();
        this.duration = duration;
    }

    @Override
    public void onStart(ClusterMembership membership) {
        executor.schedule(new TimerTask() {
            @Override
            public void run() {
                Member poll = members.poll();
                if (poll != null) {
                    membership.addMember(poll);
                    executor.schedule(this, duration.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void onClose() {
    }
}
