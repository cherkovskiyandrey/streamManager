package com.tango.stream.manager.util;


import jdk.nashorn.internal.ir.annotations.Ignore;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Stack;

@Ignore
public class TestClock extends Clock {

    private final Stack<Clock> delegates = new Stack<>();

    private final Clock delegate;

    public TestClock(Clock delegate) {
        this.delegate = delegate;
    }

    @Override
    public ZoneId getZone() {
        return getDelegate().getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return getDelegate().withZone(zone);
    }

    @Override
    public Instant instant() {
        return getDelegate().instant();
    }

    @Nonnull
    private Clock getDelegate() {
        return delegates.isEmpty() ? delegate : delegates.peek();
    }

    public void setFixed(long timeMs) {
        pushDelegate(Clock.fixed(Instant.ofEpochMilli(timeMs), getDelegate().getZone()));
    }

    public void setFixedPlusSeconds(int seconds) {
        Clock delegate = getDelegate();
        pushDelegate(Clock.fixed(delegate.instant().plusSeconds(seconds), delegate.getZone()));
    }

    public void pushDelegate(Clock delegate) {
        delegates.push(delegate);
    }

    public void reset() {
        delegates.clear();
    }
}
