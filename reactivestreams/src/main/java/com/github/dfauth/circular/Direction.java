package com.github.dfauth.circular;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public enum Direction {
    UPSTREAM,
    DOWNSTREAM;

    public boolean isDownstream() {
        return this == DOWNSTREAM;
    }

    public boolean isUpstream() {
        return this == UPSTREAM;
    }

//    public Direction onDownstream(Consumer<Direction> consumer) {
//        if(this == DOWNSTREAM) {
//            consumer.accept(this);
//        }
//        return this;
//    }
//
//    public Direction onUpstream(Consumer<Direction> consumer) {
//        if(this == UPSTREAM) {
//            consumer.accept(this);
//        }
//        return this;
//    }

    public static class DirectionLogic<T> {
        private final Direction d;
        private final Optional<T> t;

        public DirectionLogic(Direction d) {
            this(d, Optional.empty());
        }

        public DirectionLogic(Direction d, Optional<T> t) {
            this.d = d;
            this.t = t;
        }

        public static <T> DirectionLogic<T> logic(Direction d) {
            return new DirectionLogic<>(d);
        }

        public DirectionLogic<T> onDownstream(Function<Direction,T> f) {
            return d.isDownstream() ? new DirectionLogic<>(d, Optional.of(f.apply(d))) : this;
        }

        public DirectionLogic<T> onDownstream(Consumer<Direction> c) {
            if(d.isDownstream()) {
                c.accept(d);
            }
            return this;
        }

        public DirectionLogic<T> onUpstream(Function<Direction,T> f) {
            return d.isUpstream() ? new DirectionLogic<>(d, Optional.of(f.apply(d))) : this;
        }

        public DirectionLogic<T> onUpstream(Consumer<Direction> c) {
            if(d.isUpstream()) {
                c.accept(d);
            }
            return this;
        }

        public T payload() {
            return t.get();
        }
    }
}
