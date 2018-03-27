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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;

import java.net.InetAddress;
import java.util.Collection;
import java.util.UUID;
import javax.management.openmbean.CompositeData;

import com.google.common.collect.Lists;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.management.ProgressInfoCompositeData;
import org.apache.cassandra.streaming.management.SessionInfoCompositeData;
import org.junit.Before;


public class StreamManagerTest {

  private StreamManager streamManager;

  @Before
  public void setup() {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    streamManager = StreamManager.create(context);
  }

  private CompositeData sessionInfo(String host, String peer, String planId, long totalSizeToSend) throws Exception {
    UUID planUuid = UUID.fromString(planId);
    InetAddress hostAddr = InetAddress.getByName(host);
    int sessionIndex = 0;
    InetAddress peerAddr = InetAddress.getByName(peer);
    Collection<StreamSummary> receiving = Lists.newArrayList(new StreamSummary(planUuid, 0, 0));
    Collection<StreamSummary> sending = Lists.newArrayList(new StreamSummary(planUuid, 0, totalSizeToSend));
    StreamSession.State state = StreamSession.State.PREPARING;
    SessionInfo sessionInfo = new SessionInfo(hostAddr, sessionIndex, peerAddr, receiving, sending, state);
    return SessionInfoCompositeData.toCompositeData(planUuid, sessionInfo);
  }

  private CompositeData progressInfo(String planId,
                                     String peer,
                                     String fileName,
                                     String direction,
                                     long currentB,
                                     long totalB
  ) throws Exception {
    InetAddress peerAddr = InetAddress.getByName(peer);
    UUID planUuid = UUID.fromString(planId);
    int sessionIndex = 0;
    ProgressInfo.Direction dir = ProgressInfo.Direction.valueOf(direction);

    ProgressInfo progressInfo = new ProgressInfo(peerAddr, sessionIndex, fileName, dir, currentB, totalB);
    return ProgressInfoCompositeData.toCompositeData(planUuid, progressInfo);
  }

}
