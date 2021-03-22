package com.github.dfauth.circular;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

public abstract class CircularBufferSubscriber<T,V> implements Subscriber<T>, Directional {

    private static final Logger logger = LoggerFactory.getLogger(CircularBufferSubscriber.class);

    protected final Coordinator coordinator;
    protected final ForkJoinPool pool;
    protected volatile Optional<Subscription> optSubscription = Optional.empty();
    protected final CircularBuffer<SubscriberCommand<V>> circularBuffer;
    private final AtomicLong countdown = new AtomicLong(0);

    public CircularBufferSubscriber(CircularBuffer<SubscriberCommand<V>> circularBuffer, Coordinator coordinator, ForkJoinPool pool) {
        this.circularBuffer = circularBuffer;
        this.coordinator = coordinator;
        this.pool = pool;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        optSubscription = Optional.of(subscription);
    }

    public final void init() {
        optSubscription.ifPresent(subscription -> {
            Optional.of(capacity())
                    .filter(c -> c > 0)
                    .stream()
                    .peek(c -> countdown.set(c))
                    .peek(c -> subscription.request(c))
                    .forEach(c -> logger.debug("requested {} items direction {}",c,direction()));
        });
        _init();
    }

    protected abstract void _init();

    protected abstract long capacity();

    protected abstract Callable<Boolean> callableAction();

    @Override
    public final void onNext(T t) {
        try {
            _onNext(t);
        } finally {
            countdown.decrementAndGet();
            coordinator.maybeRun(direction());
        }
    }

    protected abstract void _onNext(T t);

    @Override
    public void onError(Throwable t) {
        logger.error(t.getMessage(), t);
        circularBuffer.write(SubscriberCommand.onError(t));
        countdown.decrementAndGet();
    }

    @Override
    public void onComplete() {
        logger.debug("onComplete() is called");
        circularBuffer.write(SubscriberCommand.onComplete);
        countdown.decrementAndGet();
    }

    public final void readOne() {
        long c = capacity();
        if(c > 0 && countdown.get() <= 0) {
            optSubscription.ifPresent(s -> {
                s.request(c);
                countdown.set(c);
                logger.debug("requested {} more items direction {}",c,direction());
            });
        }
    }
}
