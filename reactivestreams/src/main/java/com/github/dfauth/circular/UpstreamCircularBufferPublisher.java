package com.github.dfauth.circular;

import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;

public class UpstreamCircularBufferPublisher<T, U, V> extends CircularBufferPublisher<T, T> implements Upstream {

    private static final Logger logger = LoggerFactory.getLogger(UpstreamCircularBufferPublisher.class);

    private final BiPredicate<SubscriberCommand<U>, T> p;

    public UpstreamCircularBufferPublisher(CircularBuffer<SubscriberCommand<T>> circularBuffer, Coordinator coordinator, ForkJoinPool pool, BiPredicate<SubscriberCommand<U>,T> p) {
        super(circularBuffer, coordinator, pool);
        this.p = p;
        circularBuffer.ackWith((command, offset, offsetConsumer) ->
                optSubscriber.map(s -> {
                    boolean result = command.execute(s);
                    offsetConsumer.accept(offset+1);
                    logger.debug("executed command: "+command+" with subscriber "+s);
                    return result;
                }).orElseThrow(() -> new IllegalStateException("No subscriber found"))
        );
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        super.subscribe(subscriber);
        coordinator.onSubscription(this);
    }

    @Override
    protected long capacity() {
        return Long.MAX_VALUE;
    }

    @Override
    protected Callable<Boolean> callableAction() {
        return () -> circularBuffer.ack();
    }

    @Override
    public void request(long l) {

    }

    @Override
    public void cancel() {

    }
}
