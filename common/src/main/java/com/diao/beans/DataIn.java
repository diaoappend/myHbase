package com.diao.beans;

import java.io.Closeable;
import java.util.List;

public interface DataIn extends Closeable {
    public Object read() throws Exception;
    public <T extends Data> List<T> read(Class<T> clazz) throws Exception;
}
