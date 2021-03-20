package com.github.dfauth.circular;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface ElementFactory<U> {
    Element<U> create(U u, boolean autoAck);
    default Element<U>[] createArray(int bufferSize, boolean autoAck) {
        return IntStream.range(0, bufferSize).mapToObj(ignored -> create(null, autoAck)).collect(Collectors.toList()).toArray(new Element[bufferSize]);
    }
}
