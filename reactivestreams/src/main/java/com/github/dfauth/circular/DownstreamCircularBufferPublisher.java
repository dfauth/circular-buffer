package com.github.dfauth.circular;

import org.reactivestreams.Subscriber;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public class DownstreamCircularBufferPublisher<T> extends CircularBufferPublisher<T,T> implements Downstream {

    public DownstreamCircularBufferPublisher(CircularBuffer<SubscriberCommand<T>> circularBuffer, Coordinator coordinator, ForkJoinPool pool) {
        super(circularBuffer, coordinator, pool);
        circularBuffer.readWith((command, offset, offsetConsumer) -> tryCatch(() -> {
                    return optSubscriber.map(s -> {
                        return Optional.ofNullable(command).map(c -> {
                            offsetConsumer.accept(offset+1);
                            return c.execute(s);
                        }).orElse(false);
                    }).orElseThrow(() -> new IllegalStateException("No subscriber found"));
                },
                e -> {
                    offsetConsumer.accept(circularBuffer.getAckOffset());
                    return null;
                }));
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        super.subscribe(subscriber);
        coordinator.onSubscription(this);
    }

    @Override
    protected long capacity() {
        return 0;
    }

    @Override
    protected Callable<Boolean> callableAction() {
        return () -> circularBuffer.read();
    }

    @Override
    public void request(long l) {
    }

    @Override
    public void cancel() {
    }
}
