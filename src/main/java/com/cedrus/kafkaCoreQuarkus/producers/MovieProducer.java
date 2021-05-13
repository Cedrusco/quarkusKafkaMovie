package com.cedrus.kafkaCoreQuarkus.producers;

import com.cedrus.kafkaCoreQuarkus.config.KafkaConfig;
import com.cedrus.kafkaCoreQuarkus.models.Movie;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.util.Properties;

@Slf4j
@ApplicationScoped
public class MovieProducer {

    private static Logger logger = LoggerFactory.getLogger(MovieProducer.class);

    //there is an implicit "inject" here --> https://quarkus.io/guides/cdi#can-i-use-setter-and-constructor-injection
    private final KafkaConfig kafkaConfig;
    private final Producer<String,Movie> producer;

    public MovieProducer(KafkaConfig kafkaConfig){
        this.kafkaConfig = kafkaConfig;

        final Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfig.getBootstrapServers());
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ObjectMapperSerializer.class.getName());

        this.producer = new KafkaProducer<String, Movie>(kafkaProperties);
    }


    public void sendMovieToKafka(Movie movie){

        String key = "movie_"+movie.getTitle()+"_"+movie.getYear();

        ProducerRecord<String,Movie> movieRecord =
                new ProducerRecord<String,Movie>(kafkaConfig.getTopicName(),key,movie);

        this.producer.send(movieRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                logger.info("\nReceived new metadata: \n" +
                        "Topic: " + metadata.topic() + "\n" +
                        "KEY: " + movieRecord.key() + "\n" +
                        "VALUE: " + movieRecord.value() + "\n" +
                        "Partition: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset());
            }
        });

    }


}
