package com.github.dfauth.circular;

public interface Coordinator<T,U> {

    void onSubscribe(DownstreamCircularBufferSubscriber<T> subscriber);

    void onSubscribe(UpstreamCircularBufferSubscriber<T,U> subscriber);

    void onSubscription(DownstreamCircularBufferPublisher<U> subscriber);

    void onSubscription(UpstreamCircularBufferPublisher<U,?,?> subscriber);

    DownstreamCircularBufferPublisher<T> getDownstreamPublisher();

    DownstreamCircularBufferSubscriber<T> getDownstreamSubscriber();

    UpstreamCircularBufferPublisher<U,T,?> getUpstreamPublisher();

    UpstreamCircularBufferSubscriber<U,T> getUpstreamSubscriber();

    void maybeRun(Direction direction);

    void close();

    CircularBufferSubscriber getSubscriberFor(Direction direction);
}
