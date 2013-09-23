/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
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

package org.kiji.web;

import java.io.IOException;
import java.util.Map;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiColumnName;

/**
 * Concrete implementation of a ProducerContext more suitable for sending data back to a web
 * client. Rather than writing data the producer sends to HBase, collect the results in a list of
 * KijiRestCells that get sent back to the client (as JSON right now).
 *
 */
public class KijiWebContext implements ProducerContext {

  private KeyValueStoreReaderFactory mKVStoreFactory = null;
  private KijiScoringServerCell mOutputCell = null;
  private String mFamily = null;
  private String mQualifier = null;

  /**
   * Constructs a new KijiWebContext given the bound KV stores and the output column to "write" the
   * final results to upon completion of the producer.
   *
   * @param boundStores is the map of name to KVStore.
   * @param outputColumn is the name of the column to write the results to.
   */
  public KijiWebContext(Map<String, KeyValueStore<?, ?>> boundStores, KijiColumnName outputColumn) {
    mKVStoreFactory = KeyValueStoreReaderFactory.create(boundStores);
    mFamily = outputColumn.getFamily();
    mQualifier = outputColumn.getQualifier();
  }

  /**
   * Returns the list of written cells that would have been dumped to HBase.
   *
   * @return the list of written cells that would have been dumped to HBase.
   */
  public KijiScoringServerCell getWrittenCell() {
    return mOutputCell;
  }

  @Override
  public void close() throws IOException {
    mKVStoreFactory.close();
  }

  @Override
  public void flush() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> KeyValueStoreReader<K, V> getStore(String storeName) throws IOException {
    return mKVStoreFactory.openStore(storeName);
  }

  @Override
  public void incrementCounter(Enum<?> counter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void incrementCounter(Enum<?> counter, long amount) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void progress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStatus(String msg) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStatus() {
    return null;
  }

  @Override
  public <T> void put(T value) throws IOException {
    mOutputCell = new KijiScoringServerCell(mFamily, mQualifier, System.currentTimeMillis(), value);
  }

  @Override
  public <T> void put(long timestamp, T value) throws IOException {
    mOutputCell = new KijiScoringServerCell(mFamily, mQualifier, timestamp, value);
  }

  @Override
  public <T> void put(String qualifier, T value) throws IOException {
    mOutputCell = new KijiScoringServerCell(mFamily, qualifier, System.currentTimeMillis(), value);
  }

  @Override
  public <T> void put(String qualifier, long timestamp, T value) throws IOException {
    mOutputCell = new KijiScoringServerCell(mFamily, qualifier, timestamp, value);
  }
}
