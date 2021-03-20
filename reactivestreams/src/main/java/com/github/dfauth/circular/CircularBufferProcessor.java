package com.github.dfauth.circular;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

public abstract class CircularBufferProcessor<T,U,V> implements Processor<T, U> {

    private static final Logger logger = LoggerFactory.getLogger(CircularBufferProcessor.class);

    protected final Coordinator coordinator;
    protected final ForkJoinPool pool;
    protected volatile Optional<Subscriber<? super U>> optSubscriber = Optional.empty();
    protected volatile Optional<Subscription> optSubscription = Optional.empty();
    protected final CircularBuffer<SubscriberCommand<V>> circularBuffer;
    private final RecursiveActionSubscription s;
    private final AtomicLong countdown = new AtomicLong(0);

    public CircularBufferProcessor(CircularBuffer<SubscriberCommand<V>> circularBuffer, Coordinator coordinator, ForkJoinPool pool, Direction direction) {
        this.circularBuffer = circularBuffer;
        this.coordinator = coordinator;
        this.pool = pool;
        this.s = new RecursiveActionSubscription(pool, callableAction(), this);
    }

    @Override
    public void subscribe(Subscriber<? super U> subscriber) {
        Optional<Subscription> tmp;
        synchronized (this) {
            optSubscriber = Optional.of(subscriber);
            tmp = optSubscription;
        }
        tmp.ifPresent(s -> coordinator.init(this));
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        Optional<Subscriber<? super U>> tmp;
        synchronized (this) {
            optSubscription = Optional.of(subscription);
            tmp = optSubscriber;
        }
        tmp.ifPresent(s -> coordinator.init(this));
    }

    public final void init() {
        optSubscription.ifPresent(subscription -> {
            optSubscriber.ifPresent(subscriber -> {
                subscriber.onSubscribe(s);
                long c = capacity();
                countdown.set(c);
                subscription.request(c);
                logger.debug("requested {} items direction {}",c,direction());
            });
        });
    }

    protected abstract long capacity();

    protected abstract Callable<Boolean> callableAction();

    @Override
    public final void onNext(T t) {
        try {
            _onNext(t);
        } finally {
            countdown.decrementAndGet();
            s.runIfIdle();
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

    public abstract Direction direction();

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
