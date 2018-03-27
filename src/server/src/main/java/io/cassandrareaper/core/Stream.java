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

package io.cassandrareaper.core;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableMap;


/**
 * This class describes the state of a file exchange between two Cassandra nodes.
 */
public final class Stream {

  /**
   * String identifier of this stream.
   *
   * <p>Needs to be unique within a StreamSession.
   */
  private final String id;

  /**
   * Node on the _local_ side of the stream. The JMX notification came from this node.
   */
  private final String host;

  /**
   * Node on the _remote_ side of the stream.
   */
  private final String peer;

  /**
   * Total number of bytes to transfer from host to peer.
   */
  private final long sizeToSend;

  /**
   * Total number of bytes to transfer from peer to host.
   */
  private final long sizeToReceive;

  /**
   * Per-file total number of bytes sent.
   */
  private final ImmutableMap<String, Long> progressSent ;

  /**
   * Per-file total number of files received.
   */
  private final ImmutableMap<String, Long> progressReceived;

  /**
   * Timestamp of the last JMX notification used to update this stream.
   */
  private final long lastUpdated;

  /**
   * Indicates if the stream has completed.
   */
  private final boolean completed;

  /**
   * Indicates if the stream has completed successfully.
   */
  private final boolean success;

  /**
   * Number of bytes transferred from host to peer. Needed only to make the JSON automagic see this field.
   */
  private final long sizeSent;

  /**
   * Number of bytes transferred from peer to host. Needed only to make the JSON automagic see this field.
   */
  private final long sizeReceived;

  private Stream(Builder builder) {
    this.id = builder.id;
    this.host = builder.host;
    this.peer = builder.peer;
    this.sizeToSend = builder.sizeToSend;
    this.sizeToReceive = builder.sizeToReceive;
    this.sizeSent = builder.sizeSent;
    this.sizeReceived = builder.sizeReceived;
    this.progressSent = ImmutableMap.copyOf(builder.progressSent);
    this.progressReceived = ImmutableMap.copyOf(builder.progressReceived);
    this.lastUpdated = builder.lastUpdated;
    this.completed = builder.completed;
    this.success = builder.success;
  }

  public String getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public String getPeer() {
    return peer;
  }

  public long getSizeToSend() {
    return sizeToSend;
  }

  public long getSizeToReceive() {
    return sizeToReceive;
  }

  public long getSizeSent() {
    return sizeSent;
  }

  public long getSizeReceived() {
    return sizeReceived;
  }

  public ImmutableMap<String, Long> getProgressReceived() {
    return progressReceived;
  }

  public ImmutableMap<String, Long> getProgressSent() {
    return progressSent;
  }

  public long getLastUpdated() {
    return lastUpdated;
  }

  public boolean getCompleted() {
    return completed;
  }

  public boolean getSuccess() {
    return success;
  }

  public String toString() {
    return String.format(
        "Stream(host=%s, peer=%s, toSend=%d, sent=%d, toReceive=%d, received=%d, updated=%d, completed=%b",
        host, peer, sizeToSend, getSizeSent(), sizeToReceive, getSizeReceived(), lastUpdated, completed);
  }

  public static Stream.Builder builder() {
    return new Stream.Builder();
  }

  public static Stream.Builder builder(Stream oldStream) {
    return new Stream.Builder(oldStream);
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String id;
    private String host;
    private String peer;
    private long sizeToSend;
    private long sizeToReceive;
    private long sizeSent;
    private long sizeReceived;
    private Map<String, Long> progressSent;
    private Map<String, Long> progressReceived;
    private long lastUpdated;
    private boolean completed;
    private boolean success;

    private Builder() {}

    public Builder(Stream oldStream) {
      this.id = oldStream.getId();
      this.host = oldStream.getHost();
      this.peer = oldStream.getPeer();
      this.sizeToSend = oldStream.getSizeToSend();
      this.sizeToReceive = oldStream.getSizeToReceive();
      this.progressSent = oldStream.getProgressSent();
      this.progressReceived = oldStream.getProgressReceived();
      this.lastUpdated = oldStream.getLastUpdated();
      this.completed = oldStream.getCompleted();
      this.success = oldStream.getSuccess();
    }

    public Builder withId(String id) {
      this.id = id;
      return this;
    }

    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    public Stream.Builder withPeer(String peer) {
      this.peer = peer;
      return this;
    }

    public Stream.Builder withProgressReceived(Map<String, Long> progressReceived) {
      this.progressReceived = progressReceived;
      return this;
    }

    public Stream.Builder withSizeToReceive(long sizeToReceive) {
      this.sizeToReceive = sizeToReceive;
      return this;
    }

    public Stream.Builder withProgressSent(Map<String, Long> progressSent) {
      this.progressSent = progressSent;
      return this;
    }

    public Stream.Builder withSizeToSend(long sizeToSend) {
      this.sizeToSend = sizeToSend;
      return this;
    }

    public Stream.Builder withSizeSent(long sizeSent) {
      this.sizeSent = sizeSent;
      return this;
    }

    public Stream.Builder withSizeReceived(long sizeReceived) {
      this.sizeReceived =  sizeReceived;
      return this;
    }

    public Stream.Builder withLastUpdated(long lastUpdated) {
      this.lastUpdated = lastUpdated;
      return this;
    }

    public Stream.Builder withCompleted(boolean completed) {
      this.completed = completed;
      return this;
    }

    public Stream.Builder withSuccess(boolean success) {
      this.success = success;
      return this;
    }

    public Stream build() {
      return new Stream(this);
    }

  }

}
