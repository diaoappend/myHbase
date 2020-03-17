package com.diao.beans;

import java.io.Closeable;

public interface Consumer extends Closeable {
    public void consume();
}
