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

import io.cassandrareaper.core.Stream;

import java.util.Map;

import org.apache.cassandra.streaming.SessionInfo;

import static java.util.stream.Collectors.toMap;

public final class StreamFactory {

  private StreamFactory() {}

  public static Stream newStream(String host, SessionInfo sessionInfo, long timeStamp) {

    final String peer = sessionInfo.connecting.toString().replaceAll("/", "");

    final Map<String, Long> progressReceived = sessionInfo.getReceivingFiles()
        .stream()
        .collect(toMap(si -> si.fileName, si -> si.totalBytes));
    final long sizeReceived = progressReceived.values()
        .stream()
        .mapToLong(Long::longValue)
        .sum();

    final Map<String, Long> progressSent = sessionInfo.getSendingFiles()
        .stream()
        .collect(toMap(si -> si.fileName, si -> si.totalBytes));
    final long sizeSent = progressSent.values()
        .stream()
        .mapToLong(Long::longValue)
        .sum();

    String direction = "-";
    if (sizeReceived == 0 && sizeSent != 0) {
      direction = "->";
    }
    if (sizeReceived != 0 && sizeSent == 0) {
      direction = "<-";
    }
    if (sizeReceived != 0 && sizeSent != 0) {
      direction = "<->";
    }
    final String streamId = String.format("%s%s%s", host, direction, peer);

    return Stream.builder()
        .withId(streamId)
        .withHost(host)
        .withPeer(peer)
        .withSizeToReceive(sessionInfo.getTotalSizeToReceive())
        .withSizeToSend(sessionInfo.getTotalSizeToSend())
        .withProgressSent(progressSent)
        .withProgressReceived(progressReceived)
        .withSizeSent(sizeSent)
        .withSizeReceived(sizeReceived)
        .withLastUpdated(timeStamp)
        .withCompleted(false)
        .withSuccess(false)
        .build();
  }

}
