package com.akpanda.kinesis;

import com.akpanda.kinesis.config.KinesisClient;
import com.akpanda.kinesis.producer.kpl.KinesisKPLProducer;
import com.akpanda.kinesis.producer.sdk.KinesisSDKProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KinesisApplication implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(KinesisApplication.class.getName());
	@Value("${aws.stream.name}")
	private String streamName;

	@Autowired
	KinesisSDKProducer kinesisSDKProducer;

	@Autowired
	KinesisKPLProducer kinesisKPLProducer;
	public static void main(String[] args) {
		SpringApplication.run(KinesisApplication.class, args);

	}

	@Override
	public void run(String... args) throws Exception {
		LOG.info("Starting kinesis publisher one by one entry");
		kinesisSDKProducer.publishToStreamOneByOne(streamName);
		LOG.info("Stopping kinesis publisher one by one entry");

		LOG.info("Starting kinesis publisher all at once");
		kinesisSDKProducer.publishToStreamAllAtOnce(streamName);
		LOG.info("Stopping kinesis publisher all at once");

		LOG.info("Starting KPL unblocked");
		kinesisKPLProducer.kplProducerUnblockinhg(streamName);
		LOG.info("Stopping KPL unblocked");

		LOG.info("Starting KPL blocked");
		kinesisKPLProducer.kplProducerBlocked(streamName);
		LOG.info("Stopping KPL blocked");

		LOG.info("Starting KPL async unblocked with call back");
		kinesisKPLProducer.kplProducerAsync(streamName);
		LOG.info("Stopping KPL async unblocked with call back");
	}
}
