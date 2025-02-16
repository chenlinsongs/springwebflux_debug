package org.example.springwebflux.model;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: 2024/12/8 23:20
 * @Description:
 */
public class ImageResponse {
    private byte[] body;
    private long readableBytes;
    private long len;

    private long count;

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getReadableBytes() {
        return readableBytes;
    }

    public void setReadableBytes(long readableBytes) {
        this.readableBytes = readableBytes;
    }

    public long getLen() {
        return len;
    }

    public void setLen(long len) {
        this.len = len;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
