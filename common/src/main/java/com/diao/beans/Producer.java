package com.diao.beans;



import java.io.Closeable;

public interface Producer extends Closeable {
    public void setIn(DataIn in);
    public void setOut(DataOut out);

    public void produce();
}
