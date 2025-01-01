package org.example.springwebflux.model;

import io.netty.buffer.ByteBuf;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: 2024/12/8 22:54
 * @Description:
 */
public class Image {
    ByteBuf buf;
    long len;

    public ByteBuf getBuf() {
        return buf;
    }

    public void setBuf(ByteBuf buf) {
        this.buf = buf;
    }

    public long getLen() {
        return len;
    }

    public void setLen(long len) {
        this.len = len;
    }
}
