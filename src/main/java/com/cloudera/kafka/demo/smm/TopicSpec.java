package com.cloudera.kafka.demo.smm;

import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.Map;

@Data
@Builder
public class TopicSpec {
    private String name;
    private int numPartitions;
    private int replicationFactor;
    private Map<String,String> configs = Collections.emptyMap();
}