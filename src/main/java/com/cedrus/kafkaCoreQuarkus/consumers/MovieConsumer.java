package com.cedrus.kafkaCoreQuarkus.consumers;


import com.cedrus.kafkaCoreQuarkus.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
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
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaConfig.getBootstrapServers());
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

            this.consumer = new KafkaConsumer<String, String>(kafkaProperties);
        this.consumer.subscribe(Collections.singleton(kafkaConfig.getTopicName()));
    }


    public void receiveMovieFromKafka(ConsumerRecord<String,String> movieRecord){
        logger.info("\nGot a new movie: " + "Key: " + movieRecord.key() + "Value: " + movieRecord.value());
    }

}
