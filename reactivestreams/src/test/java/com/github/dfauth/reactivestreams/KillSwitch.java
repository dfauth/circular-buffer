package com.github.dfauth.reactivestreams;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static com.github.dfauth.function.Function2.peek;

public class KillSwitch<T> extends AbstractBaseProcessor<T,T> {

    @Override
    protected void init(Subscriber<? super T> subscriber, Subscription subscription) {
        subscriber.onSubscribe(subscription);
    }

    @Override
    public void onNext(T t) {
        optSubscriber.map(peek(s -> s.onNext(t))).orElseThrow(supplyRuntimeException());
    }

    @Override
    public void onError(Throwable t) {
        optSubscriber.map(peek(s -> s.onError(t))).orElseThrow(supplyRuntimeException());
    }

    @Override
    public void onComplete() {
        optSubscriber.map(peek(s -> s.onComplete())).orElseThrow(supplyRuntimeException());
    }

    public void shutdown() {
        onComplete();
        optSubscription.map(peek(s -> s.cancel())).orElseThrow(supplyRuntimeException());
    }

    private Supplier<? extends RuntimeException> supplyRuntimeException() {
        return () -> new IllegalStateException("misconfigured killswitch");
    }

}
