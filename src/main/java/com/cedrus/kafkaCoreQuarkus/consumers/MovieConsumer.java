package com.cedrus.kafkaCoreQuarkus.consumers;


import com.cedrus.kafkaCoreQuarkus.config.KafkaConfig;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@ApplicationScoped
public class MovieConsumer {

    private static Logger logger = LoggerFactory.getLogger(MovieConsumer.class);

    @Inject
    KafkaConfig kafkaConfig;

    private final Consumer<String,String> consumer;
    private final Deserializer<String> stringDeserializer;

    public MovieConsumer(){

        stringDeserializer = Serdes.String().deserializer();
        final Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG,"movieGroup1");

        this.consumer = new KafkaConsumer<String, String>(kafkaProperties);


    }

    volatile boolean done = false;
    volatile String last;

    public void initConsumer(@Observes StartupEvent ev){
        this.consumer.subscribe(Collections.singleton(kafkaConfig.getTopicName()));

        new Thread(() -> {
            while (! done) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

                consumerRecords.forEach(record -> {
                    logger.info("\nPolled Record:\nKEY: {} \nVALUE: {} \nPARTITION: {} \nOFFSET: {}\n",
                            record.key(), record.value(),
                            record.partition(), record.offset());
                    last = record.key() + "-" + record.value();
                });
            }
            consumer.close();
        }).start();
    }

    public void shutDownConsumer(@Observes ShutdownEvent ev){
        done = false;
    }



}
