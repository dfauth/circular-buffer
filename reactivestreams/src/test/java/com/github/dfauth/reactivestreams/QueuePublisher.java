package com.github.dfauth.reactivestreams;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public class QueuePublisher<T> implements Publisher<T>, Subscription {

    private static final Logger logger = LoggerFactory.getLogger(QueuePublisher.class);

    private BlockingQueue<T> queue;

    private Optional<Subscriber<? super T>> optSubscriber = Optional.empty();
    private Executor executor;
    private AtomicBoolean stop = new AtomicBoolean(false);
    private Duration timeout = Duration.ofSeconds(1);

    public QueuePublisher() {
        this(128);
    }

    public QueuePublisher(int size) {
        this(new ArrayBlockingQueue<>(size), Executors.newSingleThreadExecutor());
    }

    public QueuePublisher(BlockingQueue<T> queue) {
        this(queue, Executors.newSingleThreadExecutor());
    }

    public QueuePublisher(BlockingQueue<T> queue, Executor executor) {
        this.queue = queue;
        this.executor = executor;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        optSubscriber = Optional.ofNullable(subscriber);
        subscriber.onSubscribe(this);
    }

    @Override
    public void request(long l) {
        executor.execute(() -> {
            long i = l;
            while(!stop.get() && i > 0) {
                T t = tryCatch(() -> {
                    return queue.poll(100, TimeUnit.MILLISECONDS);
                });
                if(t != null) {
                    optSubscriber.ifPresent(s -> s.onNext(t));
                    i--;
                }
            }
        });
    }

    @Override
    public void cancel() {
        stop.set(true);
    }

    public BlockingQueue<T>  getQueue() {
        return queue;
    }
}
