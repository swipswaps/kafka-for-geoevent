/*
  Copyright 1995-2016 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
*/

package com.esri.geoevent.transport.kafka11;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.util.Converter;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;

class Kafka11InboundTransport extends InboundTransportBase implements Runnable {
  private static final BundleLogger LOGGER =
      BundleLoggerFactory.getLogger(Kafka11InboundTransport.class);
  private Kafka11EventConsumer consumer;
  private int numThreads;
  private String topic;
  private String groupId;

  Kafka11InboundTransport(TransportDefinition definition) throws ComponentException {
    super(definition);
  }

  public boolean isClusterable() {
    return true;
  }

  @Override
  public void run() {
    setErrorMessage("");
    setRunningState(RunningState.STARTED);
    while (isRunning()) {
      try {
        final byte[] bytes = consumer.receive();
        if (bytes != null && bytes.length > 0) {
          ByteBuffer bb = ByteBuffer.allocate(bytes.length);
          bb.put(bytes);
          bb.flip();
          byteListener.receive(bb, "");
          bb.clear();
        }
      } catch (MessagingException e) {
        LOGGER.error("", e);
      }
    }
  }

  @Override
  public void afterPropertiesSet() {
    numThreads = Converter.convertToInteger(getProperty("numThreads").getValueAsString(), 1);
    topic = getProperty("topic").getValueAsString();
    groupId = getProperty("groupId").getValueAsString();
    super.afterPropertiesSet();
  }

  @Override
  public void validate() throws ValidationException {
    super.validate();
    if (topic.isEmpty()) {
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    }
    if (groupId.isEmpty()) {
      throw new ValidationException(LOGGER.translate("GROUP_ID_VALIDATE_ERROR"));
    }
    if (numThreads < 1) {
      throw new ValidationException(LOGGER.translate("NUM_THREADS_VALIDATE_ERROR"));
    }
    //TODO validate topic exists
    //ZkClient zkClient = new ZkClient(zkConnect, 10000, 8000, ZKStringSerializer$.MODULE$);
    //// Security for Kafka was added in Kafka 0.9.0.0 -> isSecureKafkaCluster = false
    //ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
    //Boolean topicExists = AdminUtils.topicExists(zkUtils, topic);
    //zkClient.close();
    //if (!topicExists) {
    //  throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    //}
    // Init Consumer Config
    Properties props = new Properties() {

      {
        put("group.id", groupId);
      }

      {
        put("auto.commit.interval.ms", "1000");
      }
    };
    //TODO expose props
    //consumerConfig = new ConsumerConfig(props);
  }

  @Override
  public synchronized void start() throws RunningException {
    switch (getRunningState()) {
      case STOPPING:
      case STOPPED:
      case ERROR:
        connect();
        break;
    }
  }

  @Override
  public synchronized void stop() {
    disconnect("");
  }

  private synchronized void disconnect(String reason) {
    if (!RunningState.STOPPED.equals(getRunningState())) {
      setRunningState(RunningState.STOPPING);
      shutdownConsumer();
      setErrorMessage(reason);
      setRunningState(RunningState.STOPPED);
    }
  }

  private synchronized void connect() {
    disconnect("");
    setRunningState(RunningState.STARTING);
    if (consumer == null) {
      consumer = new Kafka11EventConsumer();
    }
    if (consumer.getStatusDetails().isEmpty()) // no errors reported while instantiating a consumer
    {
      consumer.setConnected();
      new Thread(this).start();
    } else {
      setRunningState(RunningState.ERROR);
      setErrorMessage(consumer.getStatusDetails());
    }
  }

  private synchronized void shutdownConsumer() {
    if (consumer != null) {
      consumer.setDisconnected(null);
      consumer.shutdown();
      consumer = null;
    }
  }

  public void shutdown() {
    shutdownConsumer();
    super.shutdown();
  }

  private class Kafka11EventConsumer extends Kafka11ComponentBase {
    private Semaphore connectionLock;
    private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    private KafkaConsumer<byte[], byte[]> consumer;
    private ExecutorService executor;

    Kafka11EventConsumer() {
      super(new EventDestination(topic));
      connectionLock = new Semaphore(2);
      Map<String, Integer> topicCountMap =
          new HashMap<String, Integer>() {
            {
              put(topic, new Integer(numThreads));
            }
          };
      consumer = null;//FIXME Consumer.createJavaConsumerConnector(consumerConfig);
      try {
        //TODO implement with KafkaConsumer
        //Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        //    consumer.createMessageStreams(topicCountMap);
        //List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        //executor = Executors.newFixedThreadPool(numThreads);
        //int threadNumber = 0;
        //for (final KafkaStream stream : streams) {
        //  try {
        //    executor.execute(new KafkaQueueingConsumer(stream, threadNumber++));
        //  } catch (Throwable th) {
        //    System.out.println(th.getMessage());
        //    setDisconnected(th);
        //  }
        //}
      } catch (Throwable th) {
        setDisconnected(th);
        setErrorMessage(th.getMessage());
      }
    }

    public synchronized void init() throws MessagingException {
    }

    byte[] receive() throws MessagingException {
      // wait to receive messages if we are not connected
      if (!isConnected()) {
        try {
          connectionLock.acquire(); // blocks execution until a connection has been recovered
        } catch (InterruptedException error) {
          ; // ignored
        }
      }
      byte[] bytes = null;
      try {
        bytes = queue.poll(100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        ; // ignore
      }
      return bytes;
    }

    @Override
    protected void setConnected() {
      if (connectionLock.availablePermits() == 0) {
        connectionLock.release();
      }
      super.setConnected();
    }

    @Override
    protected void setDisconnected(Throwable th) {
      if (connectionLock.availablePermits() == 2) {
        connectionLock.drainPermits();
      }
      super.setDisconnected(th);
    }

    public synchronized void shutdown() {
      disconnect();
      super.shutdown();
    }

    @Override
    public synchronized void disconnect() {
      if (consumer != null) {
        consumer.close();
        consumer = null;
      }
      if (executor != null) {
        executor.shutdown();
        try {
          if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
            LOGGER.info(
                "Timed out waiting for Kafka event consumer threads to shut down, exiting uncleanly");
          }
        } catch (InterruptedException e) {
          LOGGER.error(
              "Interrupted during Kafka event consumer threads shutdown, exiting uncleanly");
        }
        executor = null;
      }
      super.disconnect();
    }

    private class KafkaQueueingConsumer implements Runnable {
      //private KafkaStream<byte[], byte[]> stream;
      private int threadNumber;

      KafkaQueueingConsumer(int threadNumber) {
        //this.stream = stream;
        this.threadNumber = threadNumber;
      }

      public void run() {
        LOGGER.info("Starting Kafka consuming thread #" + threadNumber);
        while (getStatusDetails().isEmpty()) {
          if (!isConnected()) {
            try {
              connectionLock.acquire(); // blocks execution until a connection has been recovered
            } catch (InterruptedException error) {
              ; // ignored
            }
          }
          //TODO implement with KafkaConnector
          //for (ConsumerIterator<byte[], byte[]> it = stream.iterator(); it.hasNext(); ) {
          //  try {
          //    queue.offer(it.next().message(), 100, TimeUnit.MILLISECONDS);
          //  } catch (InterruptedException ex) {
          //    ; //ignore
          //  }
          //}
        }
        LOGGER.info("Shutting down Kafka consuming thread #" + threadNumber);
      }
    }
  }
}
