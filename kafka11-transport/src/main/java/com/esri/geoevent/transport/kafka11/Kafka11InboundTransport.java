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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

class Kafka11InboundTransport extends InboundTransportBase implements Runnable {
  private static final BundleLogger LOGGER =
      BundleLoggerFactory.getLogger(Kafka11InboundTransport.class);
  private Kafka11EventConsumer eventConsumer;
  private String bootstrap = "localhost:9092";
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
        final Collection<byte[]> values = eventConsumer.receive();
        for (byte[] bytes : values) {
          if (bytes != null && bytes.length > 0) {
            LOGGER.info("Message received.");
            final ByteBuffer bb = ByteBuffer.allocate(bytes.length);
            bb.put(bytes);
            bb.flip();
            byteListener.receive(bb, "");
            bb.clear();
          }
        }
      } catch (MessagingException e) {
        LOGGER.error("", e);
      }
    }
  }

  @Override
  public void afterPropertiesSet() {
    bootstrap = getProperty("bootstrap").getValueAsString();
    numThreads = Converter.convertToInteger(getProperty("numThreads").getValueAsString(), 1);
    topic = getProperty("topic").getValueAsString();
    groupId = getProperty("groupId").getValueAsString();
    super.afterPropertiesSet();
    LOGGER.info(
        String.format("Properties: bootstrap=%s numThreads=%s topic=%s groupId=%s", bootstrap,
            numThreads,
            topic, groupId));
  }

  @Override
  public void validate() throws ValidationException {
    super.validate();
    if (bootstrap == null || bootstrap.isEmpty()) {
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_VALIDATE_ERROR"));
    }
    if (topic.isEmpty()) {
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    }
    if (groupId.isEmpty()) {
      throw new ValidationException(LOGGER.translate("GROUP_ID_VALIDATE_ERROR"));
    }
    if (numThreads < 1) {
      throw new ValidationException(LOGGER.translate("NUM_THREADS_VALIDATE_ERROR"));
    }
    LOGGER.info("Consumer validated.");
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
    final RunningState runningState = getRunningState();
    LOGGER.info("Consumer Disconnecting, current state: " + runningState);
    if (!RunningState.STOPPED.equals(runningState)) {
      setRunningState(RunningState.STOPPING);
      shutdownConsumer();
      setErrorMessage(reason);
      setRunningState(RunningState.STOPPED);
    }
  }

  private synchronized void connect() {
    LOGGER.info("Consumer connecting.");
    disconnect("");
    setRunningState(RunningState.STARTING);
    if (eventConsumer == null) {
      eventConsumer = new Kafka11EventConsumer();
    }
    if (eventConsumer.getStatusDetails()
        .isEmpty()) // no errors reported while instantiating a consumer
    {
      eventConsumer.setConnected();
      new Thread(this).start();
    } else {
      setRunningState(RunningState.ERROR);
      setErrorMessage(eventConsumer.getStatusDetails());
    }
  }

  private synchronized void shutdownConsumer() {
    if (eventConsumer != null) {
      eventConsumer.setDisconnected(null);
      eventConsumer.shutdown();
      eventConsumer = null;
    }
  }

  public void shutdown() {
    shutdownConsumer();
    super.shutdown();
  }

  private class Kafka11EventConsumer extends Kafka11ComponentBase {
    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;

    Kafka11EventConsumer() {
      super(new EventDestination(topic));
      props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          ByteArrayDeserializer.class.getName());
      props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          ByteArrayDeserializer.class.getName());
      props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(10_000));
      Thread.currentThread()
          .setContextClassLoader(null); // see http://stackoverflow.com/questions/34734907/karaf-kafka-osgi-bundle-producer-issue for details
      kafkaConsumer = new KafkaConsumer<>(props);
      kafkaConsumer.subscribe(Collections.singletonList(topic));
    }

    public synchronized void init() throws MessagingException {
    }

    Collection<byte[]> receive() throws MessagingException {
      // wait to receive messages if we are not connected
      List<byte[]> values = new ArrayList<>();
      try {
        ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE);
        LOGGER.info("Consumer received " + consumerRecords.count() + " records");
        for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
          values.add(consumerRecord.value());
        }
      } catch (Exception e) {
        // ignore
        e.printStackTrace();
      }
      return values;
    }

    public synchronized void shutdown() {
      disconnect();
      super.shutdown();
    }

    @Override
    public synchronized void disconnect() {
      kafkaConsumer.close();
      super.disconnect();
    }
  }
}
