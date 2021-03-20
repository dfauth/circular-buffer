package com.github.dfauth.circular;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;

public class CircularBufferCoordinator<T,U> implements Coordinator<T,U> {

    private static final Logger logger = LoggerFactory.getLogger(CircularBufferCoordinator.class);
    private DownstreamCircularBufferProcessor<T> downstreamProcessor;
    private UpstreamCircularBufferProcessor<U,T> upstreamProcessor;
    private Optional<CircularBufferProcessor<T,?,?>> optDownstreamProcessor = Optional.empty();
    private Optional<CircularBufferProcessor<T,?,?>> optUpstreamProcessor = Optional.empty();

    public CircularBufferCoordinator(CircularBuffer<T> buffer) {
        this(buffer, (a,b) -> a.equals(b), ForkJoinPool.commonPool());
    }
    public CircularBufferCoordinator(CircularBuffer<T> buffer,
                                     BiPredicate<T,U> p,
                                     ForkJoinPool pool) {
        downstreamProcessor = new DownstreamCircularBufferProcessor(buffer, this, pool);
        upstreamProcessor = new UpstreamCircularBufferProcessor(buffer, this, pool, SubscriberCommand.OnNextSubscriberCommand.comparator(p));
    }

    @Override
    public void init(CircularBufferProcessor<T,?,?> processor) {
        if(processor == downstreamProcessor) {
            optDownstreamProcessor = Optional.of(processor);
            optUpstreamProcessor.ifPresent(p -> init());
        } else {
            optUpstreamProcessor = Optional.of(processor);
            optDownstreamProcessor.ifPresent(p -> init());
        }
    }

    public void init() {
        optUpstreamProcessor.ifPresent(p -> p.init());
        optDownstreamProcessor.ifPresent(p -> p.init());
    }

    public DownstreamCircularBufferProcessor<T> downstreamProcessor() {
        return downstreamProcessor;
    }

    public UpstreamCircularBufferProcessor<U,T> upstreamProcessor() {
        return upstreamProcessor;
    }
}
