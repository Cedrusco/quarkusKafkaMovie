package com.cedrus.kafkaCoreQuarkus.models;

import lombok.Data;

import javax.enterprise.context.ApplicationScoped;

@Data
@ApplicationScoped
public class Movie {

    public String title;
    public int year;

}