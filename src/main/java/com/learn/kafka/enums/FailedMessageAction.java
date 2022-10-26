package com.learn.kafka.enums;

public enum FailedMessageAction {
    RETRY("RETRY"),
    DEAD_LETTER("DEAD_LETTER"),
    SUCCESS("SUCCESS");

    private final String action;

    FailedMessageAction(String action) {
        this.action = action;
    }

    public String toString() {
        return this.action;
    }
}
