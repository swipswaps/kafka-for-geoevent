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

package com.esri.geoevent.transport.kafka;

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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.*;

class KafkaInboundTransport extends InboundTransportBase implements Runnable {
  private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(KafkaInboundTransport.class);
  private KafkaEventConsumer consumer;
  private Properties consumerConfig;
  private String bootstrapServers;
  private int numThreads;
  private String topic;
  private String groupId;

  KafkaInboundTransport(TransportDefinition definition) throws ComponentException {
    super(definition);
  }

  public boolean isClusterable() {
    return true;
  }

  @Override
  public void run()
  {
    setErrorMessage("");
    setRunningState(RunningState.STARTED);
    while (isRunning())
    {
      try
      {
        byte[] bytes = consumer.receive();
        if (bytes != null && bytes.length > 0) {
          ByteBuffer bb = ByteBuffer.allocate(bytes.length);
          bb.put(bytes);
          bb.flip();
          byteListener.receive(bb, "");
          bb.clear();
        }
      }
      catch (MessagingException e)
      {
        LOGGER.error("", e);
      }
    }
  }

  @Override
  public void afterPropertiesSet() {
    bootstrapServers = getProperty("bootstrapServers").getValueAsString();
    numThreads = Converter.convertToInteger(getProperty("numThreads").getValueAsString(), 1);
    topic = getProperty("topic").getValueAsString();
    groupId = getProperty("groupId").getValueAsString();
    super.afterPropertiesSet();
  }

  @Override
  public void validate() throws ValidationException {
    super.validate();
    if (bootstrapServers.isEmpty())
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_SERVERS_VALIDATE_ERROR"));
    if (topic.isEmpty())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    if (groupId.isEmpty())
      throw new ValidationException(LOGGER.translate("GROUP_ID_VALIDATE_ERROR"));
    if (numThreads < 1)
      throw new ValidationException(LOGGER.translate("NUM_THREADS_VALIDATE_ERROR"));
//    ZkClient zkClient = new ZkClient(zkConnect, 10000, 8000, ZKStringSerializer$.MODULE$);
    // Security for Kafka was added in Kafka 0.9.0.0 -> isSecureKafkaCluster = false
//    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
//    Boolean topicExists = AdminUtils.topicExists(zkUtils, topic);
//    zkClient.close();
    final Properties adminProperties = new Properties();
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    AdminClient adminClient = AdminClient.create(adminProperties);
    Optional<String> topicOption = null;
    try {
      topicOption = adminClient.listTopics().names().get().stream().filter(t -> t.equals(topic)).findAny();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("Error listing topics", e);
    }
    if (!topicOption.isPresent())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    // Init Consumer Config
    consumerConfig = new Properties()
    {
      { put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); }
      { put("group.id", groupId); }
//      { put("zookeeper.session.timeout.ms", "400"); }
//      { put("zookeeper.sync.time.ms", "200"); }
      { put("auto.commit.interval.ms", "1000"); }
    };
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
  public synchronized void stop()
  {
    disconnect("");
  }

  private synchronized void disconnect(String reason)
  {
    if (!RunningState.STOPPED.equals(getRunningState()))
    {
      setRunningState(RunningState.STOPPING);
      shutdownConsumer();
      setErrorMessage(reason);
      setRunningState(RunningState.STOPPED);
    }
  }

  private synchronized void connect()
  {
    disconnect("");
    setRunningState(RunningState.STARTING);
    if (consumer == null)
      consumer = new KafkaEventConsumer();
    if (consumer.getStatusDetails().isEmpty()) // no errors reported while instantiating a consumer
    {
      consumer.setConnected();
      new Thread(this).start();
    }
    else
    {
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

  private class KafkaEventConsumer extends KafkaComponentBase {
    private Semaphore connectionLock;
    private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>();
    private KafkaConsumer<byte[], byte[]> consumer;
    private ExecutorService executor;

    KafkaEventConsumer() {
      super(new EventDestination(topic));
      connectionLock = new Semaphore(2);

      try
      {
        executor = Executors.newFixedThreadPool(numThreads);
        int threadNumber = 0;
        for (int i = 0; i < numThreads; i++)
        {
          try
          {
            executor.execute(new KafkaQueueingConsumer(i, groupId, topic));
          }
          catch (Throwable th)
          {
            System.out.println(th.getMessage());
            setDisconnected(th);
          }
        }
      }
      catch (Throwable th)
      {
        setDisconnected(th);
        setErrorMessage(th.getMessage());
      }
    }

    public synchronized void init() throws MessagingException
    {
      ;
    }

    byte[] receive() throws MessagingException {
      // wait to receive messages if we are not connected
      if (!isConnected())
      {
        try
        {
          connectionLock.acquire(); // blocks execution until a connection has been recovered
        }
        catch (InterruptedException error)
        {
          ; // ignored
        }
      }
      byte[] bytes = null;
      try
      {
        bytes = queue.poll(100, TimeUnit.MILLISECONDS);
      }
      catch (Exception e)
      {
        ; // ignore
      }
      return bytes;
    }

    @Override
    protected void setConnected() {
      if (connectionLock.availablePermits() == 0)
        connectionLock.release();
      super.setConnected();
    }

    @Override
    protected void setDisconnected(Throwable th) {
      if (connectionLock.availablePermits() == 2)
        connectionLock.drainPermits();
      super.setDisconnected(th);
    }

    public synchronized void shutdown() {
      disconnect();
      super.shutdown();
    }

    @Override
    public synchronized void disconnect()
    {
      if (consumer != null)
      {
        consumer.close();
        consumer = null;
      }
      if (executor != null)
      {
        executor.shutdown();
        try
        {
          if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
            LOGGER.info("Timed out waiting for Kafka event consumer threads to shut down, exiting uncleanly");
        }
        catch (InterruptedException e)
        {
          LOGGER.error("Interrupted during Kafka event consumer threads shutdown, exiting uncleanly");
        }
        executor = null;
      }
      super.disconnect();
    }

    private class KafkaQueueingConsumer implements Runnable
    {
      private KafkaConsumer<byte[],byte[]> stream;
      private String groupId;
      private String topic;
      private int threadNumber;

      KafkaQueueingConsumer(int threadNumber, String groupId, String topic) {
        this.stream = new KafkaConsumer<>(consumerConfig);
        this.groupId = groupId;
        this.topic = topic;
        this.threadNumber = threadNumber;
      }

      public void run() {
        LOGGER.info("Starting Kafka consuming thread #" + threadNumber);
        consumer.subscribe(Collections.singletonList(topic));
        while (getStatusDetails().isEmpty())
        {
          if (!isConnected())
          {
            try
            {
              connectionLock.acquire(); // blocks execution until a connection has been recovered
            }
            catch (InterruptedException error)
            {
              ; // ignored
            }
          }
          for (ConsumerRecord<byte[], byte[]> it: stream.poll(Long.MAX_VALUE))
          {
            try
            {
              queue.offer(it.value(), 100, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ex)
            {
              ; //ignore
            }
          }
        }
        LOGGER.info("Shutting down Kafka consuming thread #" + threadNumber);
      }
    }
  }
}
