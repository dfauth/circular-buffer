package com.github.dfauth.circular;

import org.reactivestreams.Subscription;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public class DownstreamCircularBufferSubscriber<T> extends CircularBufferSubscriber<T,T> implements Downstream {

    public DownstreamCircularBufferSubscriber(CircularBuffer<SubscriberCommand<T>> circularBuffer, Coordinator coordinator, ForkJoinPool pool) {
        super(circularBuffer, coordinator, pool);
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
        return circularBuffer.remainingCapacity();
    }

    @Override
    protected Callable<Boolean> callableAction() {
        return null;
    }

    @Override
    protected void _onNext(T t) {
        circularBuffer.write(SubscriberCommand.onNext(t));
    }

}
