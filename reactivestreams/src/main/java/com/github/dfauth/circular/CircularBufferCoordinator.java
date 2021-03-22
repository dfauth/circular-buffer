package com.github.dfauth.circular;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;

public class CircularBufferCoordinator<T,U> implements Coordinator<T,U> {

    private static final Logger logger = LoggerFactory.getLogger(CircularBufferCoordinator.class);

    private DownstreamCircularBufferSubscriber<T> downstreamSubscriber;
    private DownstreamCircularBufferPublisher<T> downstreamPublisher;
    private UpstreamCircularBufferSubscriber<U,T> upstreamSubscriber;
    private UpstreamCircularBufferPublisher<U,T,?> upstreamPublisher;
    private volatile Optional<Boolean> port1 = Optional.empty();
    private volatile Optional<Boolean> port2 = Optional.empty();
    private volatile Optional<Boolean> port3 = Optional.empty();
    private volatile Optional<Boolean> port4 = Optional.empty();

    public CircularBufferCoordinator(CircularBuffer<T> buffer) {
        this(buffer, (a,b) -> a.equals(b), ForkJoinPool.commonPool());
    }

    public CircularBufferCoordinator(CircularBuffer<T> buffer,
                                     BiPredicate<T,U> p,
                                     ForkJoinPool pool) {
        downstreamPublisher = new DownstreamCircularBufferPublisher(buffer, this, pool);
        downstreamSubscriber = new DownstreamCircularBufferSubscriber(buffer, this, pool);
        upstreamSubscriber = new UpstreamCircularBufferSubscriber(buffer, this, pool, SubscriberCommand.OnNextSubscriberCommand.comparator(p));
        upstreamPublisher = new UpstreamCircularBufferPublisher(buffer, this, pool, SubscriberCommand.OnNextSubscriberCommand.comparator(p));
    }

    public DownstreamCircularBufferSubscriber<T> getDownstreamSubscriber() {
        return downstreamSubscriber;
    }

    @Override
    public UpstreamCircularBufferPublisher<U, T, ?> getUpstreamPublisher() {
        return upstreamPublisher;
    }

    public DownstreamCircularBufferPublisher<T> getDownstreamPublisher() {
        return downstreamPublisher;
    }

    public UpstreamCircularBufferSubscriber<U, T> getUpstreamSubscriber() {
        return upstreamSubscriber;
    }

    @Override
    public void maybeRun(Direction direction) {
        Direction.DirectionLogic.logic(direction).onDownstream(_d -> {
            downstreamPublisher.maybeRun();
        }).onUpstream(_d -> {
            upstreamPublisher.maybeRun();
        });
    }

    @Override
    public void close() {

    }

    @Override
    public CircularBufferSubscriber getSubscriberFor(Direction direction) {
        return Direction.DirectionLogic.<CircularBufferSubscriber>logic(direction).onDownstream(_d -> {
            return getDownstreamSubscriber();
        }).onUpstream(_d -> {
            return getUpstreamSubscriber();
        }).payload();
    }

    @Override
    public void onSubscribe(DownstreamCircularBufferSubscriber<T> subscriber) {
        Optional<Boolean> tmp2;
        Optional<Boolean> tmp3;
        Optional<Boolean> tmp4;
        synchronized (this) {
            port1 = Optional.of(true);
            tmp2 = port2;
            tmp3 = port3;
            tmp4 = port4;
        }
        tmp2.flatMap(_exists -> tmp3).flatMap(_exists -> tmp4).ifPresent(_exists -> init());
    }

    @Override
    public void onSubscription(DownstreamCircularBufferPublisher<U> publisher) {
        Optional<Boolean> tmp1;
        Optional<Boolean> tmp3;
        Optional<Boolean> tmp4;
        synchronized (this) {
            tmp1 = port1;
            port2 = Optional.of(true);
            tmp3 = port3;
            tmp4 = port4;
        }
        tmp1.flatMap(_exists -> tmp3).flatMap(_exists -> tmp4).ifPresent(_exists -> init());
    }

    @Override
    public void onSubscription(UpstreamCircularBufferPublisher publisher) {
        Optional<Boolean> tmp1;
        Optional<Boolean> tmp2;
        Optional<Boolean> tmp4;
        synchronized (this) {
            tmp1 = port1;
            tmp2 = port2;
            port3 = Optional.of(true);
            tmp4 = port4;
        }
        tmp1.flatMap(_exists -> tmp2).flatMap(_exists -> tmp4).ifPresent(_exists -> init());
    }

    @Override
    public void onSubscribe(UpstreamCircularBufferSubscriber subscriber) {
        Optional<Boolean> tmp1;
        Optional<Boolean> tmp2;
        Optional<Boolean> tmp3;
        synchronized (this) {
            tmp1 = port1;
            tmp2 = port2;
            tmp3 = port3;
            port4 = Optional.of(true);
        }
        tmp1.flatMap(_exists -> tmp2).flatMap(_exists -> tmp3).ifPresent(_exists -> init());
    }

    private void init() {
        downstreamSubscriber.init();
        downstreamPublisher.init();
        upstreamSubscriber.init();
        upstreamPublisher.init();
    }

}
