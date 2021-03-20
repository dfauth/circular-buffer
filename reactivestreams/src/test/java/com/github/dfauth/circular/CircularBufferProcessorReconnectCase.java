package com.github.dfauth.circular;

import com.github.dfauth.reactivestreams.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.github.dfauth.reactivestreams.TestUtils.generateListOfInt;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CircularBufferProcessorReconnectCase {

    private static final Logger logger = LoggerFactory.getLogger(CircularBufferProcessorReconnectCase.class);

    private int capacity = 4;

    @Test
    public void testACircularBufferProcessorShouldRecoverFromHavingTheDownstreamSubscriberDisconnected() throws InterruptedException, ExecutionException, TimeoutException {

        CircularBuffer<Integer> buffer = new CircularBuffer<>(capacity, false);

        CircularBufferCoordinator<Integer, Integer> coordinator = new CircularBufferCoordinator(buffer);

        UpstreamCircularBufferProcessor<Integer, Integer> upstream = coordinator.upstreamProcessor();

        DownstreamCircularBufferProcessor<Integer> downstream = coordinator.downstreamProcessor();

        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(100);
        Flux.from(new QueuePublisher<>(queue)).subscribe(downstream);

        List<Integer> elements = new ArrayList<>();
        CollectingSubscriber<Integer> collector = new CollectingSubscriber<>(elements);

        KillSwitch<Integer> killSwitch = new KillSwitch<>();

        Flux.from(downstream).subscribe(killSwitch);
        Flux.from(killSwitch).subscribe(collector);

        Flux.<Integer>empty().subscribe(upstream);
        Flux.from(upstream).subscribe(e -> {});

        List<Integer> ints = generateListOfInt(0, 7);

        ints.stream().forEach(m -> queue.offer(m));

        Thread.sleep(3000);
        killSwitch.shutdown();
        collector.get().get(5, TimeUnit.SECONDS);
        logger.info("expected: {} received: {}",ints,elements);
        assertEquals(ints, elements);

        // reconnect
        KillSwitch<Integer> killSwitch2 = new KillSwitch<>();

        List<Integer> elements2 = new ArrayList<>();
        CollectingSubscriber<Integer> collector2 = new CollectingSubscriber<>(elements2);

        Flux.from(downstream)
                .subscribe(killSwitch2);
        Flux.from(killSwitch2).subscribe(collector2);

        List<Integer> ints2 = generateListOfInt(8, 15);

        ints2.stream().forEach(m -> queue.offer(m));

        Thread.sleep(3000);
        killSwitch2.shutdown();
        collector2.get().get(5, TimeUnit.SECONDS);
        logger.info("expected: {} received: {}",ints2,elements2);
        assertEquals(ints2, elements2);
    }

}
