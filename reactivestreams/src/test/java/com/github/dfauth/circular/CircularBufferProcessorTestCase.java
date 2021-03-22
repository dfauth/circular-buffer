package com.github.dfauth.circular;

import com.github.dfauth.reactivestreams.CollectingSubscriber;
import com.github.dfauth.reactivestreams.QueuePublisher;
import com.github.dfauth.reactivestreams.TestUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.dfauth.reactivestreams.TestUtils.generateListOfInt;
import static com.github.dfauth.trycatch.TryCatch.tryCatch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class CircularBufferProcessorTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CircularBufferProcessorTestCase.class);

    private int capacity = 32;

    @Test
    public void testOneMessage() {

        tryCatch(() -> {
            CircularBuffer<String> buffer = new CircularBuffer<String>(capacity);

            CircularBufferCoordinator<Integer, Integer> coordinator = new CircularBufferCoordinator(buffer);

            UpstreamCircularBufferSubscriber<Integer, Integer> upstream = coordinator.getUpstreamSubscriber();

            Flux.<Integer>empty().subscribe(upstream);
            Flux.from(coordinator.getUpstreamPublisher()).subscribe(e -> {});

            DownstreamCircularBufferSubscriber<Integer> downstream = coordinator.getDownstreamSubscriber();

            int msg = 0;
            BlockingQueue<Integer> q = new ArrayBlockingQueue<>(10);
            Flux.from(new QueuePublisher<>(q))
                    .subscribe(downstream);
            q.offer(msg);

            List<Integer> elements = new ArrayList();
            CollectingSubscriber<Integer> collector = new CollectingSubscriber<>(elements);
            collector.addConsumer(i ->
                    collector.close()
            );
            Flux.from(coordinator.getDownstreamPublisher())
                    .subscribeWith(collector).get().get(5, TimeUnit.SECONDS);

            logger.info("expected: {} received: {}",msg,elements);
            assertEquals(Collections.singletonList(msg), elements);
        });
    }

    @Test
    public void testTwoMessages() {

        tryCatch(() -> {
            CircularBuffer<String> buffer = new CircularBuffer<String>(capacity);

            CircularBufferCoordinator<Integer, Integer> coordinator = new CircularBufferCoordinator(buffer);

            UpstreamCircularBufferSubscriber<Integer, Integer> upstream = coordinator.getUpstreamSubscriber();

            Flux.<Integer>empty().subscribe(upstream);
            Flux.from(coordinator.getUpstreamPublisher()).subscribe(e -> {});

            DownstreamCircularBufferSubscriber<Integer> downstream = coordinator.getDownstreamSubscriber();

            List<Integer> msgs = List.of(0, 1);
            BlockingQueue<Integer> q = new ArrayBlockingQueue<>(10);
            Flux.from(new QueuePublisher<>(q))
                    .subscribe(downstream);
            msgs.stream().forEach(i -> q.offer(i));

            List<Integer> elements = new ArrayList();
            CollectingSubscriber<Integer> collector = new CollectingSubscriber<>(elements);
            collector.addConsumer(i -> {
                if (elements.size() >= msgs.size()) {
                    collector.close();
                }
                    }
            );
            Flux.from(coordinator.getDownstreamPublisher())
                    .subscribeWith(collector).get().get(5, TimeUnit.SECONDS);

            logger.info("expected: {} received: {}",msgs,elements);
            assertEquals(msgs, elements);
        });
    }

    @Test
    public void testCapacityMessages() {

        tryCatch(() -> {
            CircularBuffer<Integer> buffer = new CircularBuffer<>(capacity);

            CircularBufferCoordinator<Integer, Integer> coordinator = new CircularBufferCoordinator(buffer);

            UpstreamCircularBufferSubscriber<Integer, Integer> upstream = coordinator.getUpstreamSubscriber();

            Flux.<Integer>empty().subscribe(upstream);
            Flux.from(coordinator.getUpstreamPublisher()).subscribe(e -> {});

            DownstreamCircularBufferSubscriber<Integer> downstream = coordinator.getDownstreamSubscriber();

            List<Integer> msgs = IntStream.range(0, capacity).mapToObj(i -> i).collect(Collectors.toList());
            BlockingQueue<Integer> q = new ArrayBlockingQueue<>(100);
            Flux.from(new QueuePublisher<>(q))
                    .subscribe(downstream);
            msgs.stream().forEach(i -> q.offer(i));

            List<Integer> elements = new ArrayList();
            CollectingSubscriber<Integer> collector = new CollectingSubscriber<>(elements);
            collector.addConsumer(i -> {
                if (elements.size() >= msgs.size()) {
                    collector.close();
                }
                    }
            );
            Flux.from(coordinator.getDownstreamPublisher())
                    .subscribeWith(collector).get().get(5, TimeUnit.SECONDS);

            logger.info("expected: {} received: {}",msgs,elements);
            assertEquals(msgs, elements);
        });
    }

    @Test
    public void testNTimesCapacityMessages() {

        int n = capacity * 6;

        tryCatch(() -> {
            CircularBuffer<Integer> buffer = new CircularBuffer<>(capacity, false);

            CircularBufferCoordinator<Integer, Integer> coordinator = new CircularBufferCoordinator(buffer);

            UpstreamCircularBufferSubscriber<Integer, Integer> upstream = coordinator.getUpstreamSubscriber();

            Flux.<Integer>empty().subscribe(upstream);
            Flux.from(coordinator.getUpstreamPublisher()).subscribe(e -> {});

            DownstreamCircularBufferSubscriber<Integer> downstream = coordinator.getDownstreamSubscriber();

            List<Integer> msgs = IntStream.range(0, n).mapToObj(i -> i).collect(Collectors.toList());
            BlockingQueue<Integer> q = new ArrayBlockingQueue<>(n*2);
            Flux.from(new QueuePublisher<>(q))
                    .subscribe(downstream);
            msgs.stream().forEach(i -> q.offer(i));

            List<Integer> elements = new ArrayList();
            CollectingSubscriber<Integer> collector = new CollectingSubscriber<>(elements);
            collector.addConsumer(i -> {
                logger.info("WOOZ {}",i);
                if (elements.size() >= msgs.size()) {
                    collector.close();
                }
                    }
            );
            Flux.from(coordinator.getDownstreamPublisher())
                    .subscribeWith(collector).get().get(50, TimeUnit.SECONDS);

            logger.info("expected: {} received: {}",msgs,elements);
            assertEquals(msgs.size(), elements.size());
            assertEquals(msgs, elements);
        });
    }

    @Test
    public void testACircularBufferProcessorWthAcknowledgementsShouldProcessDownstreamMessagesLessThanTheBUfferSizeEvenWhenWithoutAcknowledgements() throws InterruptedException {

        CircularBuffer<String> buffer = new CircularBuffer<String>(capacity);

        CircularBufferCoordinator<String, String> coordinator = new CircularBufferCoordinator(buffer);

        UpstreamCircularBufferSubscriber<String,String> upstream = coordinator.getUpstreamSubscriber();

        Flux.<String>empty().subscribe(upstream);
        Flux.from(coordinator.getUpstreamPublisher()).subscribe(e -> {});

        DownstreamCircularBufferSubscriber<String> downstream = coordinator.getDownstreamSubscriber();

        List<String> msgs = TestUtils.generateListOfUUID(capacity / 2);

        List<String> elements = new ArrayList<>();

        Flux.fromIterable(msgs)
                .subscribe(downstream);

        Flux.from(coordinator.getDownstreamPublisher())
                .subscribe(elements::add);

        Thread.sleep(1000);
        logger.info("expected: {} received: {}",msgs,elements);
        assertEquals(msgs, elements);
    }

    @Test
    public void testACircularBufferProcessorShouldProcessDownstreamMessagesBeyondTheBufferSizeOnlyIfAcknowledgementsAreDisabled() throws InterruptedException, TimeoutException, ExecutionException {

        int n = capacity * 6;

        CircularBuffer<String> buffer = new CircularBuffer<String>(capacity, false);

        CircularBufferCoordinator<Integer, Integer> coordinator = new CircularBufferCoordinator(buffer);

        UpstreamCircularBufferSubscriber<Integer, Integer> upstream = coordinator.getUpstreamSubscriber();

        Flux.<Integer>empty().subscribe(upstream);
        Flux.from(coordinator.getUpstreamPublisher()).subscribe(e -> {});

        DownstreamCircularBufferSubscriber<Integer> downstream = coordinator.getDownstreamSubscriber();

        List<Integer> msgs = TestUtils.generateListOfInt(0,n);

        List<Integer> elements = new ArrayList<>();
        CollectingSubscriber<Integer> collector = new CollectingSubscriber<>(elements);
        collector.addConsumer(e -> {
            if(elements.size() >= msgs.size()) {
                collector.close();
            }
        });

        BlockingQueue<Integer> q = new ArrayBlockingQueue<>(n*2);
        Flux.from(new QueuePublisher<>(q))
                .subscribe(downstream);
        msgs.stream().forEach(i -> q.offer(i));

        try {
            Flux.from(coordinator.getDownstreamPublisher())
                    .subscribeWith(collector).get().get(5, TimeUnit.SECONDS);

            logger.info("expected: {} received: {}",msgs,elements);
            assertEquals(msgs, elements);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            logger.info("expected: {} received: {}",msgs,elements);
            assertEquals(msgs, elements);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testIfAcknowledgementsAreEnabledACircularBufferProcessorBlocksUntilAcknowledgementsAreSentAfterTheBufferFills() throws InterruptedException, ExecutionException, TimeoutException {

        int n = capacity*6;

        CircularBuffer<String> buffer = new CircularBuffer<String>(capacity);

        CircularBufferCoordinator<Integer, Integer> coordinator = new CircularBufferCoordinator(buffer);

        UpstreamCircularBufferSubscriber<Integer, Integer> upstream = coordinator.getUpstreamSubscriber();

        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(400);
        QueuePublisher<Integer> qp = new QueuePublisher<>(queue);
        Flux.from(qp).subscribe(upstream);
        Flux.from(coordinator.getUpstreamPublisher()).subscribe(e -> {});

        DownstreamCircularBufferSubscriber<Integer> downstream = coordinator.getDownstreamSubscriber();

        List<Integer> msgs = generateListOfInt(0, n);

        List<Integer> elements = new ArrayList<>();
        CollectingSubscriber<Integer> collector = new CollectingSubscriber<>(elements);
        List<Integer> l = new ArrayList();
        collector.addConsumer(e -> {
            l.add(e);
        });

        Flux.fromIterable(msgs)
                .subscribe(downstream);

        collector.addConsumer(e -> {
            if(elements.size() == n) {
                collector.close();
            }
        });

        try {
            Flux.from(coordinator.getDownstreamPublisher())
                    .subscribeWith(collector).get().get(5, TimeUnit.SECONDS);

//            logger.info("expected: {} received: {}",msgs,elements);
//            assertEquals(msgs, elements);
            fail("expecting timeoutException");
        } catch (InterruptedException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            logger.info("expected: {} received: {}",msgs,elements);
            assertEquals(capacity, elements.size());
        }

        // publish the buffer-full already collected
        List<Integer> tmp = new ArrayList(l);
        tmp.stream().forEach(i -> queue.offer(i));

        try {
            Flux.from(coordinator.getDownstreamPublisher())
                    .subscribeWith(collector).get().get(5, TimeUnit.SECONDS);

            fail("expecting timeoutException");
        } catch (InterruptedException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            logger.info("expected: {} received: {}",msgs,elements);
            assertEquals(capacity, elements.size()); // TODO
        }



    }


}
