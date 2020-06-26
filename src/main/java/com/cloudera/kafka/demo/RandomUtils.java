package com.cloudera.kafka.demo;

import lombok.AllArgsConstructor;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@AllArgsConstructor
public class RandomUtils<E> {
    Map<E, Double> weights;
    private static final Random random = ThreadLocalRandom.current();

    public E getWeightedRandom() {
        return weights.entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<E,Double>(e.getKey(),-Math.log(random.nextDouble()) / e.getValue()))
                .min((e0,e1)-> e0.getValue().compareTo(e1.getValue()))
                .orElseThrow(IllegalArgumentException::new).getKey();
    }
}
