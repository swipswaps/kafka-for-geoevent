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
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.util.Converter;
import java.nio.ByteBuffer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

class Kafka11OutboundTransport extends OutboundTransportBase implements GeoEventAwareTransport {
  private static final BundleLogger LOGGER =
      BundleLoggerFactory.getLogger(Kafka11OutboundTransport.class);
  private Kafka11EventProducer producer;
  private String bootstrap = "localhost:9092";
  private String topic;
  private int partitions;
  private int replicas;

  Kafka11OutboundTransport(TransportDefinition definition) throws ComponentException {
    super(definition);
  }

  @Override
  public synchronized void receive(final ByteBuffer byteBuffer, String channelId) {
    receive(byteBuffer, channelId, null);
  }

  @Override
  public void receive(ByteBuffer byteBuffer, String channelId, GeoEvent geoEvent) {
    try {
      if (geoEvent != null) {
        if (producer == null) {
          producer = new Kafka11EventProducer(new EventDestination(topic), bootstrap);
        }
        producer.send(byteBuffer, geoEvent.hashCode());
      }
    } catch (MessagingException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("incomplete-switch")
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
    if (!RunningState.STOPPED.equals(getRunningState())) {
      disconnect("");
    }
  }

  @Override
  public void afterPropertiesSet() {
    super.afterPropertiesSet();
    shutdownProducer();
    bootstrap = getProperty("bootstrap").getValueAsString();
    topic = getProperty("topic").getValueAsString();
    partitions = Converter.convertToInteger(getProperty("partitions").getValueAsString(), 1);
    replicas = Converter.convertToInteger(getProperty("replicas").getValueAsString(), 0);
  }

  @Override
  public void validate() throws ValidationException {
    super.validate();
    if (bootstrap == null || bootstrap.isEmpty()) {
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_VALIDATE_ERROR"));
    }
    if (topic == null || topic.isEmpty()) {
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    }
    //TODO replace by removing and creating topic
    //ZkClient zkClient = new ZkClient(zkConnect, 10000, 8000, ZKStringSerializer$.MODULE$);
    //// Security for Kafka was added in Kafka 0.9.0.0 -> isSecureKafkaCluster = false
    //ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
    //if (AdminUtils.topicExists(zkUtils, topic)) {
    //  zkClient.deleteRecursive(ZkUtils.getTopicPath(topic));
    //}
    //
    //ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    //try {
    //
    //  Thread.currentThread().setContextClassLoader(null);
    //  AdminUtils.createTopic(zkUtils, topic, partitions, replicas, new Properties(),
    //      RackAwareMode.Disabled$.MODULE$);
    //} catch (Throwable th) {
    //  LOGGER.error(th.getMessage(), th);
    //  throw new ValidationException(th.getMessage());
    //} finally {
    //  Thread.currentThread().setContextClassLoader(classLoader);
    //}
    //zkClient.close();
  }

  private synchronized void disconnect(String reason) {
    setRunningState(RunningState.STOPPING);
    if (producer != null) {
      producer.disconnect();
      producer = null;
    }
    setErrorMessage(reason);
    setRunningState(RunningState.STOPPED);
  }

  private synchronized void connect() {
    disconnect("");
    setRunningState(RunningState.STARTED);
  }

  private synchronized void shutdownProducer() {
    if (producer != null) {
      producer.shutdown();
      producer = null;
    }
  }

  public void shutdown() {
    shutdownProducer();
    super.shutdown();
  }

  private class Kafka11EventProducer extends Kafka11ComponentBase {
    private KafkaProducer<byte[], byte[]> producer;

    Kafka11EventProducer(EventDestination destination, String bootstrap) {
      super(destination);
      // http://kafka.apache.org/documentation.html#producerconfigs
      props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          ByteArraySerializer.class.getName());
      props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          ByteArraySerializer.class.getName());
      props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "kafka11-for-geoevent");
      // props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
      // props.put(ProducerConfig.ACKS_CONFIG, "0");
      // props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "0");
      // props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "0");
      // props.put(ProducerConfig.RETRIES_CONFIG, "0");
      try {
        setup();
      } catch (MessagingException e) {
        setDisconnected(e);
      }
    }

    @Override
    public synchronized void init() throws MessagingException {
      if (producer == null) {
        Thread.currentThread()
            .setContextClassLoader(
                null); // see http://stackoverflow.com/questions/34734907/karaf-kafka-osgi-bundle-producer-issue for details
        producer = new KafkaProducer<>(props);
      }
    }

    public void send(final ByteBuffer bb, int h) throws MessagingException {
      // wait to send messages if we are not connected
      if (isConnected()) {
        byte[] key = new byte[4];
        key[3] = (byte) (h & 0xFF);
        key[2] = (byte) ((h >> 8) & 0xFF);
        key[1] = (byte) ((h >> 16) & 0xFF);
        key[0] = (byte) ((h >> 24) & 0xFF);
        final ProducerRecord<byte[], byte[]> record =
            new ProducerRecord<>(destination.getName(), key, bb.array());
        producer.send(record, (metadata, e) -> {
          if (e != null) {
            final String errorMsg = LOGGER.translate("KAFKA_SEND_FAILURE_ERROR", destination.getName(),
                e.getMessage());
            LOGGER.error(errorMsg);
          } else {
            LOGGER.debug("The offset of the record we just sent is: " + metadata.offset());
          }
        });
      }
    }

    @Override
    public synchronized void disconnect() {
      if (producer != null) {
        producer.close();
        producer = null;
      }
      super.disconnect();
    }

    @Override
    public synchronized void shutdown() {
      disconnect();
      super.shutdown();
    }
  }
}
