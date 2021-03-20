package com.github.dfauth.reactivestreams;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Optional;

public abstract class AbstractBaseProcessor<I,O> extends AbstractBaseSubscriber<I> implements Processor<I,O> {

    protected Optional<Subscriber<? super O>> optSubscriber = Optional.empty();

    @Override
    public synchronized void subscribe(Subscriber<? super O> subscriber) {
        optSubscriber = Optional.ofNullable(subscriber);
        optSubscription.ifPresent(s -> init(subscriber, s));
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        super.onSubscribe(subscription);
        optSubscriber.ifPresent(s -> init(s, subscription));
    }

    protected abstract void init(Subscriber<? super O> subscriber, Subscription subscription);
}
