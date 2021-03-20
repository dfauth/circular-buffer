package com.github.dfauth.reactivestreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestUtils {

    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    public static <T> List<T> generateListOfSomething(int start, int end, IntFunction<T> f) {
        return IntStream.range(start, end+1).mapToObj(f).collect(Collectors.toList());
    }

    public static List<String> generateListOfUUID(int n) {
        return generateListOfSomething(0, n, i -> UUID.randomUUID().toString());
    }

    public static List<Integer> generateListOfInt(int start, int end) {
        return generateListOfSomething(start, end, i -> i);
    }

}
