package com.github.dfauth.circular;

import java.util.concurrent.atomic.AtomicBoolean;

public class Element<U> {
    U payload;
    AtomicBoolean ack = new AtomicBoolean();

    public Element(U payload, boolean ack) {
        this.payload = payload;
        this.ack.set(ack);
    }
}
