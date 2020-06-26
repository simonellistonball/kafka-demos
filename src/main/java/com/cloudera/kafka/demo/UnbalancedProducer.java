package com.cloudera.kafka.demo;

import com.cloudera.kafka.demo.smm.CreateTopicRequest;
import com.cloudera.kafka.demo.smm.TopicSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import lombok.AllArgsConstructor;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.AuthenticationStrategy;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
public class UnbalancedProducer {

    boolean running;
    int interval;
    String bootstrap;
    String topic;
    String restUrl;
    Properties producerConfig;


    String user;
    String password;

    private static final Faker faker = new Faker();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Generate and produce messages in an unbalanced fashion to a kafka topic
     * <p>
     * Note this is not something you would ever do, but does represent a bad process.
     *
     * @param args
     */
    public static void main(String[] args) {
        Options options = new Options();

        options.addOption("b", "bootstrap-servers", true, "Kafka Bootstrap servers");
        options.addOption("r", "rest-url", true, "SMM REST endpoint");
        options.addOption("t", "topic", true, "Kafka Topic to write sample data to");
        options.addOption("i", "interval", true, "Interval between messages (in ms)");
        options.addOption("f", "properties", true, "Properties file for Kakfa Producer");
        options.addOption("u", "user", true, "SMM user name");
        options.addOption("p", "password", true, "SMM password");

        CommandLineParser parser = new DefaultParser();

        int interval;
        String bootstrap;
        String topic;
        String restUrl;
        Properties properties = new Properties();

        try {
            // parse arguments
            CommandLine cmd = parser.parse(options, args);
            if (!cmd.hasOption("bootstrap-servers")) {
                throw new ParseException("Bootstrap Servers required");
            } else {
                bootstrap = cmd.getOptionValue("bootstrap-servers");
            }
            if (!cmd.hasOption("topic")) {
                throw new ParseException("Kafka Topic required");
            } else {
                topic = cmd.getOptionValue("topic");
            }
            if (!cmd.hasOption("interval")) {
                interval = 1000;
            } else {
                interval = Integer.valueOf(cmd.getOptionValue("interval"));
            }
            if (!cmd.hasOption("rest-url")) {
                throw new ParseException("SMM Rest Server required");
            } else {
                restUrl = cmd.getOptionValue("rest-url");
            }

            if (cmd.hasOption("properties"))
                properties.load(new FileInputStream(cmd.getOptionValue("properties")));


            String user = cmd.getOptionValue("user");
            String password = cmd.getOptionValue("password");

            // run the job
            new UnbalancedProducer(true, interval, bootstrap, topic, restUrl, properties, user, password).run();

        } catch (ParseException | IOException e) {
            HelpFormatter formatter = new HelpFormatter();
            System.err.println(e.getMessage());
            formatter.printHelp(UnbalancedProducer.class.getName(), options, true);
        }
    }


    void run() throws IOException {
        try {
            smmCreateTopic(topic, 6, 2);

            producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfig);

            // get the partition count and weight on the first
            Map<Integer, Double> weights = producer.partitionsFor(topic).stream()
                    .map(e -> Pair.of(e.partition(), e.partition() == 0 ? 50.0 : 1.0))
                    .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            RandomUtils<Integer> random = new RandomUtils<Integer>(weights);

            while (running) {
                try {
                    // create a batch of records
                    for (int i = 0; i < 100; i++) {
                        Integer partition = random.getWeightedRandom();
                        producer.send(createRecord(partition));
                    }
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    running = false;
                }
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private ProducerRecord<String, String> createRecord(int partition) {
        return new ProducerRecord<>(
                topic,
                partition,
                UUID.randomUUID().toString(),
                faker.name().firstName() + " " + faker.name().lastName()
        );
    }

    /**
     * Use the SMM REST api to create a topic
     *
     * @param name
     * @param partitions
     * @param replicationFactor
     */
    void smmCreateTopic(String name, int partitions, int replicationFactor) throws Exception {
        CreateTopicRequest request = CreateTopicRequest.builder().newTopics(Arrays.asList(
                TopicSpec.builder()
                        .name(name)
                        .numPartitions(partitions)
                        .replicationFactor(replicationFactor)
                        .build())).build();
        sendRequest("/api/v1/admin/topics", objectMapper.writeValueAsString(request));
    }

    private void sendRequest(String path, String json) throws Exception {
        URL url = new URL(restUrl + path);
        HttpHost target = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());

        TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                NoopHostnameVerifier.INSTANCE);

        Registry<ConnectionSocketFactory> socketFactoryRegistry =
                RegistryBuilder.<ConnectionSocketFactory>create()
                        .register("https", sslsf)
                        .register("http", new PlainConnectionSocketFactory())
                        .build();

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        AuthScope authScope = new AuthScope(url.getHost(), url.getPort());
        credsProvider.setCredentials(authScope, new UsernamePasswordCredentials(user, password));

        BasicHttpClientConnectionManager connectionManager = new BasicHttpClientConnectionManager(socketFactoryRegistry);
        CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(sslsf)
                .setConnectionManager(connectionManager)
                .setDefaultCredentialsProvider(credsProvider)
                .build();

        try {
            AuthCache authCache = new BasicAuthCache();
            BasicScheme basicAuth = new BasicScheme();
            authCache.put(target, basicAuth);

            HttpClientContext localContext = HttpClientContext.create();
            localContext.setAuthCache(authCache);
            localContext.setCredentialsProvider(credsProvider);

            HttpPost httpPost = new HttpPost(url.toString());
            httpPost.addHeader("Content-Type", "application/json");
            StringEntity requestEntity = new StringEntity(json, StandardCharsets.UTF_8);
            requestEntity.setContentType("application/json");
            httpPost.setEntity(requestEntity);

            CloseableHttpResponse response = httpClient.execute(httpPost, localContext);
            response.getEntity().writeTo(System.out);
        } finally {
            httpClient.close();
        }
    }

}
