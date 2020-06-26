package com.cloudera.kafka.demo;

import lombok.AllArgsConstructor;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Program to artificially create some very bad leadership
 */
@AllArgsConstructor
public class UnbalancedLeaders {

    private String prefix;
    private int count;
    private String bootstrap;
    private Properties properties;

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("b", "bootstrap-servers", true, "Kafka Bootstrap servers");
        options.addOption("f", "properties", true, "Properties file for Kakfa Producer");
        options.addOption("t", "topic", true, "Prefix for the topics");
        options.addOption("n", "count", true, "Number of topics");

        CommandLineParser parser = new DefaultParser();

        String bootstrap;
        Properties properties = new Properties();

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

            String prefix = cmd.getOptionValue("topic", "unbalanced-test-topic-");
            int count = Integer.parseInt(cmd.getOptionValue("count","1"));

            new UnbalancedLeaders(prefix, count, bootstrap, properties).run();

        } catch (ParseException | IOException e) {
            HelpFormatter formatter = new HelpFormatter();
            System.err.println(e.getMessage());
            formatter.printHelp(UnbalancedLeaders.class.getName(), options, true);
        }
    }

    private void run() throws ExecutionException, InterruptedException {
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        AdminClient client = AdminClient.create(properties);

        // get broker list in-order to do horrible job of replica assignments, i.e. force the order so everyone ends up led by the same broker
        List<Integer> brokerIds = client.describeCluster().nodes().get().stream().
                map(node -> node.id()).collect(Collectors.toList());

        CreateTopicsResult newTopicsResult = client.createTopics(IntStream.range(0, 20).mapToObj(i -> {
            Map<Integer, List<Integer>> replicasAssignments =
                    IntStream.range(0, 6).boxed().collect(Collectors.toMap(Function.identity(), j -> brokerIds));
            return new NewTopic(String.format(prefix + "%d", i), replicasAssignments);
        }).collect(Collectors.toList()));

        KafkaFuture<Void> results = newTopicsResult.all();
        results.get();
    }

}
