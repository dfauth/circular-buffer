package com.github.dfauth.reactivestreams;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class StubbornPublisher<T> implements Publisher<T>, Subscription {

    private static final Logger logger = LoggerFactory.getLogger(StubbornPublisher.class);

    private List<T> tees;

    private Optional<Subscriber<? super T>> optSubscriber = Optional.empty();
    private Executor executor;
    private int i = 0;
    private AtomicBoolean stop = new AtomicBoolean(false);

    public StubbornPublisher(List<T> tees) {
        this(tees, Executors.newSingleThreadExecutor());
    }

    public StubbornPublisher(List<T> tees, Executor executor) {
        this.tees = tees;
        this.executor = executor;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        optSubscriber = Optional.ofNullable(subscriber);
        subscriber.onSubscribe(this);
    }

    @Override
    public void request(long l) {
        logger.info("received request {} i: {} stop: {}",l,i,stop.get());
        executor.execute(() -> {
            long l1 = l;
            for(;l1>0 && i < tees.size() && !stop.get();i++,l1--) {
                optSubscriber.ifPresent(s -> {
                    T t = tees.get(i);
                    s.onNext(t);
                    logger.info("published {} at position {}",t,i);
                });
            }
            stop.set(false);
        });
    }

    @Override
    public void cancel() {
        stop.set(true);
        logger.info("received cancel {}");
    }
}
