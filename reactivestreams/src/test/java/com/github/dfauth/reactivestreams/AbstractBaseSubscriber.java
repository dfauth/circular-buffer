package com.github.dfauth.reactivestreams;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Optional;

public abstract class AbstractBaseSubscriber<I> implements Subscriber<I> {

    protected Optional<Subscription> optSubscription = Optional.empty();

    @Override
    public void onSubscribe(Subscription subscription) {
        optSubscription = Optional.of(subscription);
    }

}
