package com.hmdp.utils;

public interface ILock {

    public boolean tryLock(long timeseconds);

    public void unlock();
}
