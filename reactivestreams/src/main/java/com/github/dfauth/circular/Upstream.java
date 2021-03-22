package com.github.dfauth.circular;

public interface Upstream extends Directional {

    default Direction direction() {
        return Direction.UPSTREAM;
    }
}
