package com.cedrus.kafkaCoreQuarkus.resource;

import com.cedrus.kafkaCoreQuarkus.models.Movie;
import com.cedrus.kafkaCoreQuarkus.producers.MovieProducer;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/movies")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MovieResource {

    @Inject
    MovieProducer producer;

    @POST
    public Response send(Movie movie) {
        producer.sendMovieToKafka(movie);
        // Return an 202 - Accepted response.
        return Response.accepted().build();
    }

    @GET
    public Response simpleGet(){
        return Response.accepted().entity("Working.").build();
    }
}
