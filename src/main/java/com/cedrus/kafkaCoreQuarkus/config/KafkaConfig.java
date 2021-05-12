package com.cedrus.kafkaCoreQuarkus.config;

import io.quarkus.arc.config.ConfigProperties;


@ConfigProperties(prefix = "kafka")
public class KafkaConfig {
    private String bootstrapServers;
    private String kafkaAppId;
    private String autoOffsetReset;
    private String topicName;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getKafkaAppId() {
        return kafkaAppId;
    }

    public void setKafkaAppId(String kafkaAppId) {
        this.kafkaAppId = kafkaAppId;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }
}