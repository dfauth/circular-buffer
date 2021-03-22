package com.github.dfauth.circular;

public interface Downstream extends Directional {

    default Direction direction() {
        return Direction.DOWNSTREAM;
    }
}
