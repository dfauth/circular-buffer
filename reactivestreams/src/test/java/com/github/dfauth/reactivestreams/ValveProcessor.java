package com.github.dfauth.reactivestreams;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.dfauth.function.Function2.peek;

public class ValveProcessor<T> extends AbstractBaseProcessor<T,T> implements Subscription {

    private static final Logger logger = LoggerFactory.getLogger(ValveProcessor.class);

    @Override
    protected void init(Subscriber<? super T> subscriber, Subscription subscription) {
        subscriber.onSubscribe(this);
    }

    @Override
    public void onNext(T t) {
        optSubscriber.map(peek(s -> {
            s.onNext(t);
            logger.info("Valve: onNext({})",t);
        })).orElseThrow(() -> new IllegalStateException("no subscriber - has the stream been set up correctly?"));
    }

    @Override
    public void onError(Throwable t) {
        optSubscriber.map(peek(s -> s.onError(t))).orElseThrow(() -> new IllegalStateException("no subscriber - has the stream been set up correctly?"));
    }

    @Override
    public void onComplete() {
        optSubscriber.map(peek(s -> s.onComplete())).orElseThrow(() -> new IllegalStateException("no subscriber - has the stream been set up correctly?"));
    }

    @Override
    public void request(long l) {
        // ignore so we can control this
    }

    @Override
    public void cancel() {
        // we will allow downstream to cancel
        optSubscription.map(peek(s -> s.cancel())).orElseThrow(() -> new IllegalStateException("no subscription - has the stream been set up correctly?"));
    }

    public void requestUpstream(long l) {
        optSubscription.map(peek(s -> s.request(l))).orElseThrow(() -> new IllegalStateException("no subscription - has the stream been set up correctly?"));
    }

    public void cancelUpstreamCompleteDownstream() {
        cancel();
        optSubscriber.map(peek(s -> s.onComplete())).orElseThrow(() -> new IllegalStateException("no subscriber - has the stream been set up correctly?"));
    }

}
