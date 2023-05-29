package com.akpanda.kinesis.config;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class KinesisClient {

    AmazonKinesis amazonKinesisClient;

    @Value("${aws.stream.name}")
    private String streamName;

    @Bean
    public void setupKinesis(){
        amazonKinesisClient = new AmazonKinesisClient();
        Optional<String> streamExists = amazonKinesisClient.listStreams().getStreamNames().stream().filter(x -> x.equalsIgnoreCase(streamName)).findAny();
        if(streamExists.isEmpty()){
            // create a stream with one shard
            amazonKinesisClient.createStream(streamName,1);
        }
    }

    public AmazonKinesis getAmazonKinesisClient() {
        return amazonKinesisClient;
    }

    public void setAmazonKinesisClient(AmazonKinesis amazonKinesisClient) {
        this.amazonKinesisClient = amazonKinesisClient;
    }
}
