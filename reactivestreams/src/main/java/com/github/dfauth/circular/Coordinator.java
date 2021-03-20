package com.github.dfauth.circular;

public interface Coordinator<T,U> {
    void init(CircularBufferProcessor<T,?,?> processor);

    DownstreamCircularBufferProcessor<T> downstreamProcessor();

    UpstreamCircularBufferProcessor<U,T> upstreamProcessor();
}
