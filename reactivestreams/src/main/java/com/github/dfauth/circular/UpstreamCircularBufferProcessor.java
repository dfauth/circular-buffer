package com.github.dfauth.circular;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;

public class UpstreamCircularBufferProcessor<T,U> extends CircularBufferProcessor<T, U, U> {

    private static final Logger logger = LoggerFactory.getLogger(UpstreamCircularBufferProcessor.class);
    private final BiPredicate<SubscriberCommand<U>, T> p;

    public UpstreamCircularBufferProcessor(CircularBuffer<SubscriberCommand<U>> circularBuffer, Coordinator coordinator, ForkJoinPool pool, BiPredicate<SubscriberCommand<U>,T> p) {
        super(circularBuffer, coordinator, pool, Direction.UPSTREAM);
        this.p = p;
        circularBuffer.ackWith((command, offset, offsetConsumer) ->
            optSubscriber.map(s -> {
                boolean result = command.execute(s);
                offsetConsumer.accept(offset+1);
                logger.debug("executed command: "+command+" with subscriber "+s);
                return result;
            }).orElseThrow(() -> new IllegalStateException("No subscriber found"))
        );
    }

    @Override
    protected long capacity() {
        return Long.MAX_VALUE;
    }

    @Override
    protected Callable<Boolean> callableAction() {
        return () -> circularBuffer.ack();
    }

    @Override
    protected void _onNext(T t) {
        circularBuffer.acknowledge(t, p);
    }

    @Override
    public void onError(Throwable ex) {
        logger.error(ex.getMessage(), ex);
    }

    @Override
    public void onComplete() {
        logger.info("onComplete()");
    }

    @Override
    public Direction direction() {
        return Direction.UPSTREAM;
    }

}
