package com.cedrus.kafkaCoreQuarkus.models;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class MovieDeserializer extends ObjectMapperDeserializer<Movie> {

    public MovieDeserializer(){
        super(Movie.class);
    }
}
