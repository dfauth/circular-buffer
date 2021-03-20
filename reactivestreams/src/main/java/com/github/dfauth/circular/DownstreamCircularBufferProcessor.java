package com.github.dfauth.circular;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public class DownstreamCircularBufferProcessor<T> extends CircularBufferProcessor<T,T,T> {

    private static final Logger logger = LoggerFactory.getLogger(DownstreamCircularBufferProcessor.class);

    public DownstreamCircularBufferProcessor(CircularBuffer<SubscriberCommand<T>> circularBuffer, Coordinator coordinator, ForkJoinPool pool) {
        super(circularBuffer, coordinator, pool, Direction.DOWNSTREAM);
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
    protected void _onNext(T t) {
        int n = circularBuffer.write(SubscriberCommand.onNext(t));
        logger.debug("_onNext("+t+") is called n: {}",n);
    }

    @Override
    public Direction direction() {
        return Direction.DOWNSTREAM;
    }

    @Override
    protected long capacity() {
        return circularBuffer.remainingCapacity();
    }

    @Override
    protected Callable<Boolean> callableAction() {
        return () -> circularBuffer.read();
    }
}
