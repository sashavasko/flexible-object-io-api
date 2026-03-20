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

import org.sv.flexobject.arrow.ArrowFlightConf;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * A Flight Server that provides access to the InMemoryStore.
 */
public class MemoryStoreServer extends Configured implements Tool, AutoCloseable, Runnable {

  private static final Logger logger = LogManager.getLogger(MemoryStoreServer.class);

  FlightServer flightServer;
  Location location;
  BufferAllocator allocator;
  MemoryStore mem;

  ArrowFlightConf conf = new ArrowFlightConf();

  public MemoryStoreServer() {
  }

  public MemoryStoreServer(ArrowFlightConf arrowFlightConf) {
    conf = arrowFlightConf;
  }

  protected void init(){
    init(null);
  }
  protected void init(BufferAllocator rootAllocator){
    if (flightServer == null) {
      this.allocator = rootAllocator != null
              ? rootAllocator.newChildAllocator("flight-server", 0, Long.MAX_VALUE)
              : conf.getAllocator();
      this.location = conf.getLocation();
      this.mem = new MemoryStore(this.allocator, location);
      this.flightServer = FlightServer.builder(allocator, location, mem).build();
    }
  }

  public Location getLocation() {
    return location;
  }

  public int getPort() {
    return this.flightServer.getPort();
  }

  public void start() throws IOException {
    flightServer.start();
  }

  public void awaitTermination() throws InterruptedException {
    flightServer.awaitTermination();
  }

  public MemoryStore getStore() {
    return mem;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(mem, flightServer, allocator);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null)
      this.conf.from(conf);
  }

  @Override
  public void run() {
    init();

    try {
      start();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start server for configuration:" + conf.toString(), e);
    }

    try {
      awaitTermination();
    } catch (InterruptedException e) {
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    /**
     * Optionally :
     * try(BufferAllocator a = new RootAllocator(Long.MAX_VALUE)){
     *  init(a);
     *  run();
     * }
     *
     */
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        logger.info("Exiting...");
        this.close();
      } catch (Exception e) {
        logger.error("Exception shutting down server", e);
      }
    }));

    run();
    return 0;
  }

  public ArrowFlightConf getArrowConf() {
    return conf;
  }
}
