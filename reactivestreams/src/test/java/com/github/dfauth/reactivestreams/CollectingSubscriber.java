package com.github.dfauth.reactivestreams;

import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CollectingSubscriber<I> extends AbstractBaseSubscriber<I> implements Supplier<CompletableFuture<List<I>>> {

    private CompletableFuture<List<I>> f = new CompletableFuture<>();
    private List<I> elements;
    private List<Consumer<I>> consumers = new ArrayList<>();

    public CollectingSubscriber() {
        this(new ArrayList());
    }

    public CollectingSubscriber(List<I> elements) {
        this.elements = elements;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        super.onSubscribe(subscription);
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(I i) {
        elements.add(i);
        consumers.stream().forEach(c -> c.accept(i));
    }

    @Override
    public void onError(Throwable t) {
        f.completeExceptionally(t);
    }

    @Override
    public void onComplete() {
        f.complete(elements);
    }

    @Override
    public CompletableFuture<List<I>> get() {
        return f;
    }

    public void addConsumer(Consumer<I> consumer) {
        this.consumers.add(consumer);
    }

    public List<I> getElements() {
        return elements;
    }

    public CompletableFuture<List<I>> notify(BiPredicate<I,List<I>> p) {
        CompletableFuture<List<I>> f1 = new CompletableFuture<>();
        addConsumer(e -> Optional.ofNullable(e).filter(_e -> p.test(_e,elements)).ifPresent(_e -> f1.complete(elements)));
        return f1;
    }

    public void close() {
        optSubscription.ifPresent(s -> s.cancel());
        onComplete();
    }
}
