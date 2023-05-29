package com.akpanda.kinesis.producer.sdk;

import com.akpanda.kinesis.config.KinesisClient;
import com.akpanda.kinesis.domain.GeoLocation;
import com.akpanda.kinesis.domain.VirtualLocation;
import com.amazonaws.services.kinesis.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

@Component
public class KinesisSDKProducer {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisClient.class.getName());

    @Autowired
    KinesisClient kinesisClient;

    public void publishToStreamOneByOne(String streamName){
        boolean streamIsReadyToAcceptMessages = checkStreamStatus(streamName);
        if(streamIsReadyToAcceptMessages){
            for(int i =0;i<3;i++){
                String partitionKey = "partitionKey1";
                PutRecordRequest putRecordRequest = new PutRecordRequest();;
                String geoLocationJson = (new GeoLocation()).geoLocationToJson();
                putRecordRequest.setStreamName(streamName);
                putRecordRequest.setData(ByteBuffer.wrap(geoLocationJson.getBytes()));
                putRecordRequest.setPartitionKey(partitionKey);
                PutRecordResult result = kinesisClient.getAmazonKinesisClient().putRecord(putRecordRequest);
                LOG.info("Put result "+ result);
            }
        }
        else{
            LOG.error("Stream is not ready to be published");
        }
    }

    public void publishToStreamAllAtOnce(String streamName){
        boolean streamIsReadyToAcceptMessages = checkStreamStatus(streamName);
        if(streamIsReadyToAcceptMessages){
            PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
            List<PutRecordsRequestEntry> putRecordsRequestEntryList = new LinkedList<>();
            for(int i =0;i<30;i++){
                String partitionKey = "partitionKey1";
                PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();;
                String virLocationToJson = (new VirtualLocation()).virLocationToJson();
                putRecordsRequestEntry.setData(ByteBuffer.wrap(virLocationToJson.getBytes()));
                putRecordsRequestEntry.setPartitionKey(partitionKey);
                putRecordsRequestEntryList.add(putRecordsRequestEntry);
            }
            putRecordsRequest.setRecords(putRecordsRequestEntryList);
            // In PUt Records this need to be set explictly for all records at once
            // whil in singular put record this is set per request
            putRecordsRequest.setStreamName(streamName);
            try{
                PutRecordsResult result = kinesisClient.getAmazonKinesisClient().putRecords(putRecordsRequest);
                LOG.info("Put result "+ result);
            }
            catch (Exception e){
                e.printStackTrace();
            }

        }
        else{
            LOG.error("Stream is not ready to be published");
        }
    }

    private boolean checkStreamStatus(String streamName) {
        boolean streamIsReady = false;
        for(int i =0;i<3;i++){
            Optional<StreamSummary> streamSummary = kinesisClient.getAmazonKinesisClient().listStreams()
                .getStreamSummaries().stream().filter(x -> streamName.equalsIgnoreCase(x.getStreamName())).findFirst();
            if(streamSummary.isPresent() && streamSummary.get().getStreamStatus().equalsIgnoreCase("Active")) {
                streamIsReady = true;
                break;
            }
            else{
                // Three tries at interval of 1 minute to check stream status
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return streamIsReady;
    }

}
