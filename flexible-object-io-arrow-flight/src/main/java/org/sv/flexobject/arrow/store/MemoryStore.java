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

import com.carfax.arrow.util.TicketUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing.
 */
public class MemoryStore implements FlightProducer, AutoCloseable {

  public static final Logger logger = LogManager.getLogger(MemoryStore.class);

  private final ConcurrentMap<FlightDescriptor, MemoryStoreFlightHolder> holders = new ConcurrentHashMap<>();
  private final BufferAllocator allocator;
  private Location location;

  /**
   * Constructs a new instance.
   *
   * @param allocator The allocator for creating new Arrow buffers.
   * @param location The location of the storage.
   */
  public MemoryStore(BufferAllocator allocator, Location location) {
    super();
    this.allocator = allocator;
    this.location = location;
  }

  protected class Putter implements Runnable {
    private final FlightStream flightStream;
    private final VectorSchemaRoot root;
    private final Schema schema;
    private final StreamListener<PutResult> ackStream;

    public Putter(FlightStream stream, final StreamListener<PutResult> ackStream) {
      this.flightStream = stream;
      this.ackStream = ackStream;
      root = stream.getRoot();
      schema = stream.getSchema();
    }

    @Override
    public void run() {
      Stream.StreamCreator creator = null;
      boolean success = false;
      try {
        logger.debug("Root schema:" + root.getSchema());
        final MemoryStoreFlightHolder h = holders.computeIfAbsent(
                flightStream.getDescriptor(),
                t -> new MemoryStoreFlightHolder(allocator, t, schema, flightStream.getDictionaryProvider()));

        creator = h.addStream(schema);

        VectorUnloader unloader = new VectorUnloader(root);
        int i = 0;
        while (flightStream.next()) {
          ackStream.onNext(PutResult.metadata(flightStream.getLatestMetadata()));
          ArrowRecordBatch batch = unloader.getRecordBatch();
          ++i;
          logger.debug("Batch #" + i + " length: "  + batch.getLength());
          creator.add(batch);
        }
        // Closing the stream will release the dictionaries
        flightStream.takeDictionaryOwnership();
        creator.complete();
        success = true;
      } finally {
        if (!success) {
          creator.drop();
        }
      }
    }
  }


  /**
   * Update the location after server start.
   *
   * <p>Useful for binding to port 0 to get a free port.
   */
  public void setLocation(Location location) {
    this.location = location;
  }

  @Override
  public void getStream(CallContext context, Ticket ticket,
      ServerStreamListener listener) {
    logger.debug("getStream(" + context.toString() + ", ticket:" + TicketUtils.toString(ticket) + ")");
    getStream(ticket).sendTo(allocator, listener);
  }

  /**
   * Returns the appropriate stream given the ticket (streams are indexed by path and an ordinal).
   */
  public Stream getStream(Ticket t) {
    MemoryStoreTicket example = MemoryStoreTicket.from(t);
    FlightDescriptor d = FlightDescriptor.path(example.getPath());
    MemoryStoreFlightHolder h = holders.get(d);
    if (h == null) {
      throw new IllegalStateException("Unknown ticket.");
    }

    return h.getStream(example);
  }

  @Override
  public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
    logger.debug("listFlights(" + context.toString() + ", " + criteria.toString() + ",...)");
    try {
      for (MemoryStoreFlightHolder h : holders.values()) {
        listener.onNext(h.getFlightInfo(location));
      }
      listener.onCompleted();
    } catch (Exception ex) {
      listener.onError(ex);
    }
  }

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    MemoryStoreFlightHolder h = holders.get(descriptor);
    if (h == null) {
      throw new IllegalStateException("Unknown descriptor.");
    }

    return h.getFlightInfo(location);
  }

  @Override
  public Runnable acceptPut(CallContext context,
      final FlightStream flightStream, final StreamListener<PutResult> ackStream) {

    logger.debug("acceptPut(" + flightStream.getDescriptor().getPath() + ")");
    return new Putter(flightStream, ackStream);
  }

  @Override
  public void doAction(CallContext context, Action action,
      StreamListener<Result> listener) {
    logger.debug("doAction(" + context.toString() + ", " + action.toString() + ")");

    switch (action.getType()) {
      case "drop": {
        // not implemented.
        listener.onNext(new Result(new byte[0]));
        listener.onCompleted();
        break;
      }
      default: {
        listener.onError(CallStatus.UNIMPLEMENTED.toRuntimeException());
      }
    }
  }

  @Override
  public void listActions(CallContext context,
      StreamListener<ActionType> listener) {
    listener.onNext(new ActionType("get", "pull a stream. Action must be done via standard get mechanism"));
    listener.onNext(new ActionType("put", "push a stream. Action must be done via standard put mechanism"));
    listener.onNext(new ActionType("drop", "delete a flight. Action body is a JSON encoded path."));
    listener.onCompleted();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(holders.values());
    holders.clear();
  }

}
