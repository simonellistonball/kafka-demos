package com.cloudera.kafka.demo.smm;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class CreateTopicRequest {
    private List<TopicSpec> newTopics;
}
