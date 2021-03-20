package com.github.dfauth.circular;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CircularBufferTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CircularBufferTestCase.class);

    @Test
    public void testObjectsAreReadBackInTheOrderInserted() {
        int capacity = 32;
        CircularBuffer<String> buffer = new CircularBuffer<String>((String uuid, boolean autoAck) -> new Element<String>(uuid, autoAck), capacity);

        final Optional<Integer>[] _offset = new Optional[]{Optional.empty()};
        final String[] _msg = {null};
        buffer.readWith((e, offset, offsetConsumer) -> {
            logger.info("received {} at offset {}",e,offset);
            _offset[0] = Optional.ofNullable(offset);
            _msg[0] = e;
            offsetConsumer.accept(offset+1);
            return true;
        });

        List<String> msgs = new ArrayList<>();
        for(int i=0;i<3;i++) {
            msgs.add(UUID.randomUUID().toString());
        }

        buffer.write(msgs.get(0));
        buffer.read();
        assertEquals(_offset[0],Optional.of(0));
        assertEquals(_msg[0],msgs.get(0));

        buffer.write(msgs.get(1));
        buffer.read();
        assertEquals(_offset[0],Optional.of(1));
        assertEquals(_msg[0],msgs.get(1));
        buffer.write(msgs.get(2));
        buffer.read();
        assertEquals(_offset[0],Optional.of(2));
        assertEquals(_msg[0],msgs.get(2));

    }

    @Test
    public void testThatObjectsAreDequeuedOnlyAfterBeingAcknowledged() {
        int capacity = 32;
        CircularBuffer<String> buffer = new CircularBuffer<String>((String uuid, boolean autoAck) -> new Element<String>(uuid, autoAck), capacity);

        AtomicReference<Optional<Integer>> _offset = new AtomicReference<>();
        AtomicReference<String> _msg = new AtomicReference<>();
        buffer.readWith((e, offset, offsetConsumer) -> {
            logger.info("received {} at offset {}",e,offset);
            _offset.set(Optional.ofNullable(offset));
            _msg.set(e);
            offsetConsumer.accept(offset+1);
            return true;
        });

        AtomicReference<String> _ack = new AtomicReference<>();
        buffer.ackWith(e -> {
                _ack.set(e);
        });

        List<String> msgs = new ArrayList<>();
        for(int i=0;i<3;i++) {
            msgs.add(UUID.randomUUID().toString());
        }

        buffer.write(msgs.get(0));
        buffer.read();
        buffer.ack();
        {
            int offset = 0;
            assertEquals(Optional.of(offset), _offset.get());
            assertEquals(msgs.get(offset), _msg.get());
            assertEquals(null,_ack.get());
        }

        buffer.acknowledge(msgs.get(0), (s1,s2) -> s1.compareTo(s2) == 0);

        buffer.write(msgs.get(1));
        buffer.read();
        buffer.ack();
        {
            int offset = 1;
            assertEquals(Optional.of(offset), _offset.get());
            assertEquals(msgs.get(offset), _msg.get());
            assertEquals(msgs.get(offset-1),_ack.get());
        }

        buffer.acknowledge(msgs.get(1), (s1,s2) -> s1.compareTo(s2) == 0);

        buffer.write(msgs.get(2));
        buffer.read();
        buffer.ack();
        buffer.acknowledge(msgs.get(2), (s1,s2) -> s1.compareTo(s2) == 0);
        buffer.ack();
        {
            int offset = 2;
            assertEquals(Optional.of(offset), _offset.get());
            assertEquals(msgs.get(offset), _msg.get());
            assertEquals(msgs.get(offset),_ack.get());
        }
    }

}
