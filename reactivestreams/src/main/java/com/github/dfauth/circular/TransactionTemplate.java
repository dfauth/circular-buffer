package com.github.dfauth.circular;

import java.util.function.Consumer;

public interface TransactionTemplate<T, R> {
    R process(T t, int offset, Consumer<Integer> offsetConsumer);
}
