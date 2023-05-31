package com.akpanda.kinesis.producer.kpl;

import com.akpanda.kinesis.KinesisApplication;
import com.akpanda.kinesis.config.KinesisClient;
import com.akpanda.kinesis.domain.GeoLocation;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
public class KinesisKPLProducer {
    @Autowired
    KinesisClient kinesisClient;

    private static final Logger LOG = LoggerFactory.getLogger(KinesisKPLProducer.class.getName());

    public void kplProducerUnblockinhg(String streamName){
        // Put some records
        for (int i = 0; i < 10; ++i) {
            String partitionKey = "partitionKey1";
            String geoLocationJson = (new GeoLocation("kplPrducerUnblocking")).geoLocationToJson();
            kinesisClient.getKinesisProducer().addUserRecord(streamName,partitionKey, ByteBuffer.wrap(geoLocationJson.getBytes()));
        }
    }

    public void kplProducerBlocked(String streamName){

        List<Future<UserRecordResult>> putFutures = new LinkedList<Future<UserRecordResult>>();
        // Put some records
        for (int i = 0; i < 10; ++i) {
            String partitionKey = "partitionKey1";
            String geoLocationJson = (new GeoLocation("kplProducerBlocked")).geoLocationToJson();
            putFutures.add(
                    kinesisClient.getKinesisProducer()
                            .addUserRecord(streamName,partitionKey, ByteBuffer.wrap(geoLocationJson.getBytes())));

        }
        for(Future<UserRecordResult> userRecordResultFuture: putFutures){
            UserRecordResult result = null; // this does block
            try {
                result = userRecordResultFuture.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
            if (result.isSuccessful()) {
                LOG.info("Result of publishing :" +result.getSequenceNumber()+" shard id :"+result.getShardId());
            } else {
                for (Attempt attempt : result.getAttempts()) {
                    LOG.info("Failed message details");
                    LOG.info(attempt.getErrorCode(),attempt.getErrorMessage(),attempt.getDelay(),attempt.getDuration());
                }
            }
        }
    }


    // Things to consider.
    //1. KPL with Async requires executor framework or some sort of thread pool
    //2. The callback variable needs to be passed in
    public void kplProducerAsync(String streamName){

        List<Future<UserRecordResult>> putFutures = new LinkedList<Future<UserRecordResult>>();

        FutureCallback<UserRecordResult> futuresCallback = new FutureCallback<UserRecordResult>() {
            @Override public void onFailure(Throwable t) {
                LOG.info("Error while producing", t.getStackTrace());
            };
            @Override public void onSuccess(UserRecordResult result) {
                /* Respond to the success */
                LOG.info("Producer success");
            };
        };


        final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();
        final Runnable putOneRecord = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; ++i) {
                    String partitionKey = "partitionKey1";
                    String geoLocationJson = (new GeoLocation("kplProducerAsync")).geoLocationToJson();
                    ListenableFuture<UserRecordResult> futurePutRecord = kinesisClient.getKinesisProducer()
                            .addUserRecord(streamName,partitionKey, ByteBuffer.wrap(geoLocationJson.getBytes()));

                    // If the Future is complete by the time we call addCallback, the callback will be invoked immediately.
                    Futures.addCallback(futurePutRecord,futuresCallback, callbackThreadPool);
                }
            }
        };
        callbackThreadPool.submit(putOneRecord);

    }
}
