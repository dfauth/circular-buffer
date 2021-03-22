package com.github.dfauth.circular;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public class RecursiveActionSubscription implements Subscription {

    private static final Logger logger = LoggerFactory.getLogger(RecursiveActionSubscription.class);

    private final ForkJoinPool pool;
    private final Callable<Boolean> callable;
    private final CircularBufferPublisher publisher;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicBoolean idle = new AtomicBoolean(true);
    private final AtomicLong countdown = new AtomicLong(0L);

    public RecursiveActionSubscription(ForkJoinPool pool, Callable<Boolean> callable, CircularBufferPublisher publisher) {
        this.pool = pool;
        this.callable = callable;
        this.publisher = publisher;
    }

    @Override
    public void request(long l) {
        cancelled.set(false);
        countdown.set(l);
        pool.execute(createAction());
        debug(() -> "request "+l+" "+ publisher.direction().name());
    }

    private RecursiveAction createAction() {
        return new RecursiveAction() {
            @Override
            protected void compute() {
                idle.set(false);
                debug(() -> "executing "+RecursiveActionSubscription.this);
                if(cancelled.get()) {
                    debug(() -> "cancelled "+RecursiveActionSubscription.this);
                    return;
                }
                if(countdown.get() <= 0) {
                    debug(() -> "countdown completed "+RecursiveActionSubscription.this);
                    return;
                }
                // recursively execute this task l times
                try {
                    if(callable.call()) {
                        // success; reduce by one
                        countdown.decrementAndGet();
                        debug(() -> "compute complete "+RecursiveActionSubscription.this);
                        publisher.getCoordinator().getSubscriberFor(publisher.direction()).readOne();
                        pool.execute(createAction());
                    } else {
                        idle.set(true);
                        debug(() -> "nothing to compute direction "+RecursiveActionSubscription.this);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        };
    }

    @Override
    public void cancel() {
        cancelled.set(true);
    }

    public void runIfIdle() {
        debug(() -> "runIfIdle "+this);
        Optional.of(idle.compareAndSet(true, false))
                .filter(e -> e)
                .ifPresent(ignored -> {
                    debug(() -> "scheduled for execution "+RecursiveActionSubscription.this);
                    pool.execute(createAction());
                });
    }

    @Override
    public String toString() {
        return "["+this.getClass().getSimpleName()
                +" cancelled: "+cancelled.get()
                +" idle: "+idle.get()
                +" countdown: "+countdown.get()
                +" direction: "+ publisher.direction().name()
                +"]";
    }

    private void debug(Callable<String> c) {
        tryCatch(() -> {
            if(logger.isDebugEnabled()) {
                logger.debug(c.call());
            }
        });
    }
}
