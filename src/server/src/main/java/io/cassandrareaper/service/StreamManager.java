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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.StreamSession;
import io.cassandrareaper.jmx.JmxProxy;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.cassandra.streaming.management.StreamStateCompositeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;


/**
 * This class implements the functionality needed for receiving,
 * handling and serving streaming-related notifications.
 */
public final class StreamManager {

  /**
   * TODO
   */
  private static final Logger LOG = LoggerFactory.getLogger(StreamManager.class);

  private final AppContext context;

  private LoadingCache<Node, List<StreamSession>> streamSessionCache = CacheBuilder.newBuilder()
      .maximumSize(1000L)
      .expireAfterWrite(10, TimeUnit.MILLISECONDS)
      .build(new CacheLoader<Node, List<StreamSession>>() {
        public List<StreamSession> load(Node node) throws ReaperException {
          return pullStreamInfo(node);
        }
      });

  private StreamManager(AppContext context) {
    this.context = context;
  }

  public static StreamManager create(AppContext context) {
    return new StreamManager(context);
  }

  public List<StreamSession> listStreams(Node node) throws ReaperException {
    return pullStreamInfo(node);
  }

  private List<StreamSession> pullStreamInfo(Node node) throws ReaperException {
    try {
      LOG.debug("Pulling streams for node {}", node);
      JmxProxy jmxProxy = context.jmxConnectionFactory.connect(node, context.config.getJmxConnectionTimeoutInSeconds());

      List<StreamSession> streamSessions = jmxProxy.listStreams().stream()
          .map(StreamStateCompositeData::fromCompositeData)
          .map(streamState -> StreamSessionFactory.fromStreamState(node.getHostname(), streamState))
          .collect(toList());

      return streamSessions;
    } catch (InterruptedException | ReaperException e) {
      LOG.info("Pulling streams  failed: {}", e.getMessage());
      throw new ReaperException(e);
    }
  }

}
