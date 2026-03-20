/*
 * Cloned from flight-core integration tests licensed as follows :
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sv.flexobject.arrow.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Throwables;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * POJO object used to demonstrate how an opaque ticket can be generated.
 */
@JsonSerialize
public class MemoryStoreTicket {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final List<String> path;
  private final int ordinal;

  // uuid to ensure that a stream from one node is not recreated on another node and mixed up.
  private final String uuid;

  /**
   * Constructs a new instance.
   *
   * @param path Path to data
   * @param ordinal A counter for the stream.
   * @param uuid  A unique identifier for this particular stream.
   */
  @JsonCreator
  public MemoryStoreTicket(@JsonProperty("path") List<String> path, @JsonProperty("ordinal") int ordinal,
                           @JsonProperty("uuid") String uuid) {
    super();
    Preconditions.checkArgument(ordinal >= 0);
    this.path = path;
    this.ordinal = ordinal;
    this.uuid = uuid;
  }

  public List<String> getPath() {
    return path;
  }

  public int getOrdinal() {
    return ordinal;
  }

  public String getUuid() {
    return uuid;
  }

  /**
   * Deserializes a new instance from the protocol buffer ticket.
   */
  public static MemoryStoreTicket from(Ticket ticket) {
    try {
      return MAPPER.readValue(ticket.getBytes(), MemoryStoreTicket.class);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   *  Creates a new protocol buffer Ticket by serializing to JSON.
   */
  public Ticket toTicket() {
    try {
      return new Ticket(MAPPER.writeValueAsBytes(this));
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ordinal;
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MemoryStoreTicket other = (MemoryStoreTicket) obj;
    if (ordinal != other.ordinal) {
      return false;
    }
    if (path == null) {
      if (other.path != null) {
        return false;
      }
    } else if (!path.equals(other.path)) {
      return false;
    }
    if (uuid == null) {
      if (other.uuid != null) {
        return false;
      }
    } else if (!uuid.equals(other.uuid)) {
      return false;
    }
    return true;
  }


}
