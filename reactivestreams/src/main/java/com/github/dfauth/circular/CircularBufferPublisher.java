package com.github.dfauth.circular;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

public abstract class CircularBufferPublisher<T, V> implements Publisher<T>, Subscription, Directional {

    private static final Logger logger = LoggerFactory.getLogger(CircularBufferPublisher.class);

    protected final Coordinator<T,V> coordinator;
    protected final ForkJoinPool pool;
    protected volatile Optional<Subscriber<? super T>> optSubscriber = Optional.empty();
    protected final CircularBuffer<SubscriberCommand<V>> circularBuffer;
    private final RecursiveActionSubscription s;
    private final AtomicLong countdown = new AtomicLong(0);

    public CircularBufferPublisher(CircularBuffer<SubscriberCommand<V>> circularBuffer, Coordinator<T,V> coordinator, ForkJoinPool pool) {
        this.circularBuffer = circularBuffer;
        this.coordinator = coordinator;
        this.pool = pool;
        this.s = new RecursiveActionSubscription(pool, callableAction(), this);
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        optSubscriber = Optional.of(subscriber);
    }

    public final void init() {
        optSubscriber.ifPresent(subscriber -> {
            subscriber.onSubscribe(s);
        });
    }

    protected abstract long capacity();

    protected abstract Callable<Boolean> callableAction();

    public void maybeRun() {
        s.runIfIdle();
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }

}
