package com.github.dfauth.circular;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

public class CircularBuffer<T> {

    private static final Logger logger = LoggerFactory.getLogger(CircularBuffer.class);
    private final Element<T>[] buffer;
    private final int bufferSize;
    private final ElementFactory<T> factory;
    private AtomicInteger writeOffset = new AtomicInteger(0);
    private AtomicInteger readOffset = new AtomicInteger(0);
    private AtomicInteger ackOffset = new AtomicInteger(0);
    private final boolean acknowledged;
    private Optional<TransactionTemplate<T,Boolean>> optReadTemplate = Optional.empty();
    private Optional<TransactionTemplate<T, Boolean>> optAckTemplate = Optional.empty();
    private final long sleep;

    public CircularBuffer(int capacity) {
        this(capacity, true);
    }

    public CircularBuffer(int capacity, boolean acknowledged) {
        this((payload, ack) -> new Element<>(payload, ack), capacity, acknowledged);
    }

    public CircularBuffer(ElementFactory<T> factory, int capacity) {
        this(factory, capacity, true);
    }

    public CircularBuffer(ElementFactory<T> factory, int capacity, boolean acknowledged) {
        this(factory, capacity, acknowledged, 500);
    }

    public CircularBuffer(ElementFactory<T> factory, int capacity, boolean acknowledged, long sleep) {
        this.bufferSize = capacity;
        this.factory = factory;
        this.buffer = factory.createArray(bufferSize, !acknowledged);
        this.acknowledged = acknowledged;
        this.sleep = sleep;
    }

    public int getWriteOffset() {
        return writeOffset.get();
    }

    public int getReadOffset() {
        return readOffset.get();
    }

    public int getAckOffset() {
        return ackOffset.get();
    }

    public int write(T t) {
        return Optional.ofNullable(t).map(_t -> {
            this.buffer[writeOffset.get() % bufferSize] = factory.create(_t, !acknowledged);
            logger.debug("wrote _t: "+_t+" at offset "+writeOffset.get());
            return writeOffset.incrementAndGet();
        }).orElseGet(() -> {
            logger.warn("ignored attempt to write null object to buffer: "+t);
            return writeOffset.get();
        });
    }

    public void readWith(Consumer<T> t) {
        this.optReadTemplate = Optional.of(new PassThroughTemplate(t));
    }

    public void readWith(TransactionTemplate<T,Boolean> t) {
        this.optReadTemplate = Optional.of(t);
    }

    public boolean read() {
        return optReadTemplate.map(t -> {
            boolean result = false;
            try {
                logger.debug("read() readOffset: "+readOffset.get()+" writeOffset: "+writeOffset.get()+" ackOffset: "+ackOffset.get());
                if(readOffset.get() >= writeOffset.get()) {
                    return false;
                }
                int offset = readOffset.get();
                result = t.process(buffer(offset).payload, offset, o -> readOffset.set(o));
                logger.debug("read() result: "+result+" from payload "+buffer(offset).payload+" at offset "+offset+" readOffset: "+readOffset.get()+" writeOffset: "+writeOffset.get()+" ackOffset: "+ackOffset.get());
            } catch (RuntimeException e) {
                logger.error(e.getMessage(), e);
                result = false;
            } finally {
                return result;
            }
        }).orElse(false);
    }

    public void ackWith(Consumer<T> t) {
        this.optAckTemplate = Optional.of(new PassThroughTemplate(t));
    }

    public void ackWith(TransactionTemplate<T,Boolean> t) {
        this.optAckTemplate = Optional.of(t);
    }

    public boolean ack() {
        AtomicBoolean result = new AtomicBoolean(false);
        return optAckTemplate.map(t -> {
            try {
                if(ackOffset.get() == readOffset.get()) {
                    Thread.sleep(sleep);  // nothing to process; do not consume all the CPU
                    return false;
                }
                int offset = ackOffset.get();
                result.set(Optional.of(buffer(offset)).filter(e -> e.ack.get()).map(e -> t.process(e.payload, offset, o -> ackOffset.set(o))).orElse(false));
            } catch (RuntimeException e) {
                logger.error(e.getMessage(), e);
            } finally {
                return result.get();
            }
        }).orElse(false);
    }

    public <V> void acknowledge(V v, BiPredicate<T,V> p) {
        int offset = ackOffset.get();
        acknowledge(v, p, offset);
    }

    private <V> void acknowledge(V v, BiPredicate<T,V> p, int offset) {
        if(offset >= readOffset.get()) {
            logger.warn("element "+v+" not found for acknowledgement offset: "+offset);
            return;
        }
        Element<T> e = buffer(offset);
        if(p.test(e.payload, v)) {
            e.ack.set(true);
        } else {
            acknowledge(v, p, offset+1);
        }
    }

    private Element<T> buffer(int offset) {
        return this.buffer[offset % bufferSize];
    }

    public long remainingCapacity() {
        return bufferSize - awaitingAcknowledgement();
    }

    public long capacity() {
        return bufferSize;
    }

    public long awaitingAcknowledgement() {
        if(acknowledged) {
            return writeOffset.get() - ackOffset.get();
        } else {
            return writeOffset.get() - readOffset.get();
        }
    }

    public void close(Throwable ex) {
        // TODO
    }

    public void close() {
        // TODO
    }

    private class PassThroughTemplate implements TransactionTemplate<T, Boolean> {

        private final Consumer<T> consumer;

        public PassThroughTemplate(Consumer<T> c) {
            this.consumer = c;
        }

        @Override
        public Boolean process(T t, int offset, Consumer<Integer> offsetConsumer) {
            try {
                consumer.accept(t);
            } catch (RuntimeException e) {
                logger.error(e.getMessage(), e);
            } finally {
                offsetConsumer.accept(offset+1);
                return true;
            }
        }
    }
}
