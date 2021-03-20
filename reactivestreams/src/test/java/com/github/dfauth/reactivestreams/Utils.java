package com.github.dfauth.reactivestreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.dfauth.function.Function2.peek;

public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static <T> UnaryOperator<T> logger(String s) {
        return peek(e ->
                logger.info(String.format(s,e))
        );
    }

    public static <T> Flux<T> fromQueue(Queue<T> queue) {
        return Flux.generate(sink -> {
            if(queue.peek() != null) {
                sink.next(queue.poll());
            }
        });
    }


}
