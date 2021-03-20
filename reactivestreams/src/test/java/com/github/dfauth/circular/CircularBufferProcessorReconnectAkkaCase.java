package com.github.dfauth.circular;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.function.Function2;
import akka.stream.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.github.dfauth.reactivestreams.TestUtils.generateListOfInt;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CircularBufferProcessorReconnectAkkaCase {

    private static final Logger logger = LoggerFactory.getLogger(CircularBufferProcessorReconnectAkkaCase.class);

    private int capacity = 4;
    private ActorSystem system = ActorSystem.create(this.getClass().getSimpleName());
    private Materializer materializer = ActorMaterializer.create(system);

    @Test
    public void testACircularBufferProcessorShouldRecoverFromHavingTheDownstreamSubscriberDisconnected() throws InterruptedException, ExecutionException, TimeoutException {

        CircularBuffer<Integer> buffer = new CircularBuffer<>(capacity, false);

        CircularBufferCoordinator<Integer, Integer> coordinator = new CircularBufferCoordinator(buffer);

        UpstreamCircularBufferProcessor<Integer, Integer> upstream = coordinator.upstreamProcessor();

        DownstreamCircularBufferProcessor<Integer> downstream = coordinator.downstreamProcessor();

        List<Integer> elements = new ArrayList<>();
//        CollectingSubscriber<Integer> collector = new CollectingSubscriber<>(elements);


        SharedKillSwitch killSwitch = KillSwitches.shared("killSwitch");
//        SourceQueueWithComplete<Integer> queue = Source.<Integer>queue(100, OverflowStrategy.fail())
//                .via(Flow.fromProcessor(() -> downstream))
//                .via(killSwitch.flow())
//                .to(Sink.fromSubscriber(collector)).run(materializer);

        Function2<List<Integer>, Integer, List<Integer>> f = (l, e) -> {
            l.add(e);
            return l;
        };
        Pair<SourceQueueWithComplete<Integer>, CompletionStage<List<Integer>>> pair = Source.<Integer>queue(100, OverflowStrategy.fail())
                .via(Flow.fromProcessor(() -> downstream))
                .via(killSwitch.flow())
                .toMat(Sink.fold(elements, f), Keep.both()).run(materializer);

        SourceQueueWithComplete<Integer> queue = pair.first();
        CompletionStage<List<Integer>> f1 = pair.second();

        Source.<Integer>empty().via(Flow.fromProcessor(() -> upstream)).to(Sink.ignore()).run(materializer);

        List<Integer> ints = generateListOfInt(0, 7);

        ints.stream().forEach(m -> queue.offer(m));

        Thread.sleep(3000);
        killSwitch.shutdown();
        f1.toCompletableFuture().get(5, TimeUnit.SECONDS);
        logger.info("expected: {} received: {}",ints,elements);
        assertEquals(ints, elements);

        Thread.sleep(5 * 1000);
        logger.info("DownstreamCircularBufferProcessor WOOZ1");
        Thread.sleep(5 * 1000);

        // reconnect
        SharedKillSwitch killSwitch2 = KillSwitches.shared("killSwitch-2");

        List<Integer> elements2 = new ArrayList<>();
//        CollectingSubscriber<Integer> collector2 = new CollectingSubscriber<>(elements2);

        CompletionStage<List<Integer>> f2 = Source.fromPublisher(downstream)
                .via(killSwitch2.flow())
                .toMat(Sink.fold(elements2, f), Keep.right())
                .run(materializer);

        List<Integer> ints2 = generateListOfInt(8, 15);

        ints2.stream().forEach(m -> queue.offer(m));

        Thread.sleep(3000);
        killSwitch2.shutdown();
        f2.toCompletableFuture().get(5, TimeUnit.SECONDS);
        logger.info("expected: {} received: {}",ints2,elements2);
        assertEquals(ints2, elements2);
    }

}
