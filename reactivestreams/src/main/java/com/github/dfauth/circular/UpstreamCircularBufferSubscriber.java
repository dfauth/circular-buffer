package com.github.dfauth.circular;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;

public class UpstreamCircularBufferSubscriber<T,U> extends CircularBufferSubscriber<T,U> implements Upstream {

    private static final Logger logger = LoggerFactory.getLogger(UpstreamCircularBufferSubscriber.class);

    private final BiPredicate<SubscriberCommand<U>, T> p;

    public UpstreamCircularBufferSubscriber(CircularBuffer<SubscriberCommand<U>> circularBuffer, Coordinator coordinator, ForkJoinPool pool, BiPredicate<SubscriberCommand<U>,T> p) {
        super(circularBuffer, coordinator, pool);
        this.p = p;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        super.onSubscribe(subscription);
        this.coordinator.onSubscribe(this);
    }

    @Override
    protected void _init() {
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
    protected void _onNext(T t) {
        circularBuffer.acknowledge(t, p);
    }

    @Override
    public void onError(Throwable ex) {
        logger.error(ex.getMessage(), ex);
    }

    @Override
    public void onComplete() {
        logger.info("onComplete()");
    }

}
