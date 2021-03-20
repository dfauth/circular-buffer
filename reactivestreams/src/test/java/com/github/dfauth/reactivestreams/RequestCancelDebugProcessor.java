package com.github.dfauth.reactivestreams;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class RequestCancelDebugProcessor<T> implements Processor<T,T>, Subscription {

    private static final Logger logger = LoggerFactory.getLogger(RequestCancelDebugProcessor.class);

    private final String name;
    private Optional<Subscriber<? super T>> optSubscriber = Optional.empty();
    private Optional<Subscription> optSubscription = Optional.empty();

    public RequestCancelDebugProcessor(String name) {
        this.name = name;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        optSubscriber = Optional.of(subscriber);
        optSubscription.ifPresent(s -> init());
    }

    protected void init() {
        optSubscriber.ifPresent(s -> optSubscription.ifPresent(s1 -> s.onSubscribe(this)));
        logger.info(name+" initialised");
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        optSubscription = Optional.of(subscription);
        optSubscriber.ifPresent(s -> init());
    }

    @Override
    public void onNext(T t) {
        optSubscriber.ifPresent(s -> s.onNext(t));
    }

    @Override
    public void onError(Throwable throwable) {
        optSubscriber.ifPresent(s -> s.onError(throwable));
        logger.info(name+" onError("+throwable+")");
    }

    @Override
    public void onComplete() {
        optSubscriber.ifPresent(s -> s.onComplete());
        logger.info(name+" onComplete");
    }

    @Override
    public void request(long l) {
        optSubscription.ifPresent(s -> s.request(l));
        logger.info(name+" request("+l+")");
    }

    @Override
    public void cancel() {
        optSubscription.ifPresent(s -> s.cancel());
        logger.info(name+" cancel");
    }
}
