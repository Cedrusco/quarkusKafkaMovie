package com.cedrus.kafkaCoreQuarkus.consumers;


import com.cedrus.kafkaCoreQuarkus.config.KafkaConfig;
import com.cedrus.kafkaCoreQuarkus.models.Movie;
import com.cedrus.kafkaCoreQuarkus.models.MovieDeserializer;
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

    private final Consumer<String, Movie> consumer;
    private final Deserializer<String> stringDeserializer;

    @Inject
    KafkaConfig kafkaConfig;

    public MovieConsumer(KafkaConfig kafkaConfig){

        stringDeserializer = Serdes.String().deserializer();
        final Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaConfig.getBootstrapServers());
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG,"movieGroup1");
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,MovieDeserializer.class.getName());

        this.consumer = new KafkaConsumer<String, Movie>(kafkaProperties);

    }

    volatile boolean done = false;
    volatile String last;

    public void initConsumer(@Observes StartupEvent ev){
        this.consumer.subscribe(Collections.singleton(kafkaConfig.getTopicName()));
        new Thread(() -> {
            while (! done) {
                final ConsumerRecords<String, Movie> consumerRecords = consumer.poll(Duration.ofSeconds(1));

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
