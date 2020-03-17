package com.diao.beans;

import java.io.Closeable;

public interface DataOut extends Closeable {
    public void write(Object obj) throws Exception;
    public void write(String string) throws Exception;

}
