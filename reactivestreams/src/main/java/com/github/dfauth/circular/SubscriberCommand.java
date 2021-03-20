package com.github.dfauth.circular;

import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiPredicate;

public interface SubscriberCommand<T> {

    Logger logger = LoggerFactory.getLogger(SubscriberCommand.class);

    boolean execute(Subscriber<? super T> subscriber);

    static <T> SubscriberCommand<T> onError(Throwable ex) {
        return new OnErrorSubscriberCommand<>(ex);
    }

    static <T> SubscriberCommand<T> onNext(T t) {
        return new OnNextSubscriberCommand<>(t);
    }

    SubscriberCommand onComplete = new OnCompleteSubscriberCommand<>();

    class OnCompleteSubscriberCommand<T> implements SubscriberCommand<T> {

        @Override
        public boolean execute(Subscriber<? super T> subscriber) {
            subscriber.onComplete();
            logger.debug("onComplete call on subscriber: "+subscriber);
            return false;
        }
    }

    class OnErrorSubscriberCommand<T> implements SubscriberCommand<T> {

        private Throwable t;

        public OnErrorSubscriberCommand(Throwable t) {
            this.t = t;
        }

        @Override
        public boolean execute(Subscriber<? super T> subscriber) {
            subscriber.onError(t);
            logger.debug("onError("+t.getMessage()+") called on subscriber: "+subscriber);
            return false;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()+"["+t+"]";
        }
    }

    class OnNextSubscriberCommand<T> implements SubscriberCommand<T> {

        public final T t;

        public static <T, V> BiPredicate<SubscriberCommand<T>, V> comparator(BiPredicate<T,V> p) {
            return (cmd, v) -> {
                if(cmd instanceof SubscriberCommand.OnNextSubscriberCommand) {
                    return p.test(((SubscriberCommand.OnNextSubscriberCommand<T>) cmd).t, v);
                } else {
                    return false;
                }
            };
        }

        public OnNextSubscriberCommand(T t) {
            this.t = t;
        }

        @Override
        public boolean execute(Subscriber<? super T> subscriber) {
            subscriber.onNext(t);
            logger.debug("onNext("+t+") called on subscriber: "+subscriber);
            return true;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()+"["+t+"]";
        }
    }
}
