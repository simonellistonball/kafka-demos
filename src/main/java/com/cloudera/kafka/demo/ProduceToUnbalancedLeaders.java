package com.cloudera.kafka.demo;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import lombok.AllArgsConstructor;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AllArgsConstructor
public class ProduceToUnbalancedLeaders {

    private boolean running;

    private String prefix;
    private int count;
    private String bootstrap;
    private Properties producerConfig;
    private int interval;

    private static Name fakerName = new Faker().name();

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("b", "bootstrap-servers", true, "Kafka Bootstrap servers");
        options.addOption("f", "properties", true, "Properties file for Kakfa Producer");
        options.addOption("t", "topic", true, "Prefix for the topics");
        options.addOption("n", "count", true, "Number of topics");
        options.addOption("i", "interval", true, "Interval between messages (in ms)");

        CommandLineParser parser = new DefaultParser();

        String bootstrap;
        Properties properties = new Properties();
        int interval = 1000;

        try {
            // parse arguments
            CommandLine cmd = parser.parse(options, args);
            if (!cmd.hasOption("bootstrap-servers")) {
                throw new ParseException("Bootstrap Servers required");
            } else {
                bootstrap = cmd.getOptionValue("bootstrap-servers");
            }
            if (cmd.hasOption("properties"))
                properties.load(new FileInputStream(cmd.getOptionValue("properties")));

            if (cmd.hasOption("interval"))
                interval = Integer.valueOf(cmd.getOptionValue("interval"));


            String prefix = cmd.getOptionValue("topic", "unbalanced-test-topic-");
            int count = Integer.parseInt(cmd.getOptionValue("count","1"));
            // run the job
            new ProduceToUnbalancedLeaders(true, prefix, count, bootstrap, properties, interval).run();

        } catch (ParseException | IOException e) {
            HelpFormatter formatter = new HelpFormatter();
            System.err.println(e.getMessage());
            formatter.printHelp(UnbalancedLeaders.class.getName(), options, true);
        }
    }

    private void run() throws ExecutionException, InterruptedException {
        producerConfig.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig);

        try {
            while (running) {
                for (int j = 0; j< 1000; j++) {
                    List<Future<RecordMetadata>> futures = IntStream.range(0, count).boxed().map(i ->
                            new ProducerRecord<String, String>(
                                    String.format(prefix + "%d", i),
                                    UUID.randomUUID().toString(),
                                    fakerName.firstName() + " " + fakerName.lastName()))
                            .map(record -> producer.send(record))
                            .collect(Collectors.toList());

                    for (Future<RecordMetadata> future : futures) {
                        future.get();
                    }
                }
                Thread.sleep(interval);
            }
        } finally {
            producer.close();
        }

    }
}
