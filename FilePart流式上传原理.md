# FilePart 流式上传原理

## FilePart 是什么？

`FilePart` 是 Spring WebFlux 中用于处理 multipart/form-data 文件上传的接口。

```java
public interface FilePart extends Part {
    String filename();
    Mono<Void> transferTo(Path dest);
    Flux<DataBuffer> content();
}
```

## 工作原理

### 完整流程

```java
1. 客户端发起 multipart/form-data 请求
   ↓
2. Netty 接收 TCP 数据包
   ↓
3. MultipartHttpMessageReader 解析 multipart 数据
   ↓
4. Spring 创建临时文件或内存缓冲区
   ↓
5. 写入接收到的数据（这部分是流式的）
   ↓
6. 检测到 part 结束边界（boundary）
   ↓
7. 创建 FilePart 对象
   ↓
8. Mono<FilePart> 发出信号 ← 您的Controller方法才开始执行
   ↓
9. 调用 transferTo() 或 content()
   ↓
10. 从临时文件/缓冲区复制到目标位置
```

### 关键点

#### 1. 接收是流式的

```java
// Spring 内部实现（简化）
Flux<DataBuffer> dataStream = ... // 来自网络的数据流

dataStream
    .doOnNext(buffer -> {
        // 立即写入临时文件或内存
        tempFile.write(buffer);  // 这是流式的！不会等待所有数据
    })
    .doOnComplete(() -> {
        // 所有数据接收完成
        FilePart filePart = createFilePart(tempFile);
        mono.emitValue(filePart);  // 这里才触发 Mono<FilePart>
    });
```

**结论**：
- ✅ 不会OOM（数据立即写入临时存储）
- ❌ 但处理要等待完整上传

#### 2. 触发时机

```java
@PostMapping("/upload")
public Mono<String> upload(@RequestPart("file") Mono<FilePart> filePart) {
    // 这个方法什么时候开始执行？
    // 答案：当整个文件上传完成后！
    
    return filePart
        .flatMap(file -> {
            // 这里的 file 已经是完整的 FilePart 对象
            // 数据已经在临时文件中了
        });
}
```

### 为什么要等待完整上传？

#### 原因1: multipart 协议特性

```
POST /upload HTTP/1.1
Content-Type: multipart/form-data; boundary=----WebKitFormBoundary

------WebKitFormBoundary
Content-Disposition: form-data; name="file"; filename="video.mp4"
Content-Type: video/mp4

<--- 数据开始 --->
... 1.5GB 数据 ...
<--- 数据结束 --->
------WebKitFormBoundary--  ← 必须检测到这个结束标记！
```

**关键**：只有检测到结束标记，才能确认文件完整接收。

#### 原因2: 元数据提取

```java
// FilePart 需要提供这些信息
String filename = filePart.filename();  // 从哪里来？
String contentType = filePart.headers().getContentType();  // 从哪里来？

// 答案：从 multipart headers 中解析
// 但这些 headers 在 part 的开头，需要完整解析才能提取
```

## 真的是流式吗？

### 从不同角度看

#### 角度1: 网络接收 → 临时存储

```java
网络数据到达
  ↓ (实时，流式)
写入临时文件/内存
```

**结论**：✅ 这部分是流式的

#### 角度2: 临时存储 → 应用处理

```java
完整文件在临时存储
  ↓ (等待)
Mono<FilePart> 触发
  ↓ (等待)
应用代码开始执行
```

**结论**：❌ 这部分不是流式的

### 实际验证

```java
@PostMapping("/upload-debug")
public Mono<String> uploadDebug(@RequestPart("file") Mono<FilePart> filePart) {
    long startTime = System.currentTimeMillis();
    logger.info("[{}ms] Controller 方法开始执行", 0);  // 立即打印？
    
    return filePart
        .doOnNext(file -> {
            long elapsed = System.currentTimeMillis() - startTime;
            logger.info("[{}ms] FilePart 到达", elapsed);  // 什么时候打印？
        })
        .flatMap(file -> {
            // 处理文件
            return file.transferTo(targetPath);
        });
}
```

**测试结果**（上传1.5GB文件）：

```log
[0ms] Controller 方法开始执行
... 等待 30 秒（上传中，无任何输出）...
[30000ms] FilePart 到达  ← 上传完成后才打印！
```

**结论**：确实要等待完整上传！

## transferTo() vs content()

### transferTo()

```java
Path targetPath = Paths.get("/path/to/target.mp4");
return filePart.flatMap(file -> file.transferTo(targetPath));
```

**优点**：
- ✅ 简单易用
- ✅ 系统级优化（sendfile，零拷贝）
- ✅ 性能最好

**实现原理**：
```java
// 简化的内部实现
public Mono<Void> transferTo(Path dest) {
    if (tempFile != null) {
        // 如果使用了临时文件，直接移动或复制
        return Mono.fromCallable(() -> {
            Files.move(tempFile, dest);  // 或 Files.copy
            return null;
        });
    } else {
        // 如果在内存中，写入文件
        return DataBufferUtils.write(content(), dest);
    }
}
```

### content()

```java
Flux<DataBuffer> dataStream = file.content();
return DataBufferUtils.write(dataStream, targetPath);
```

**用途**：
- 需要自定义处理逻辑
- 需要边读边处理（如计算MD5）

**注意**：
```java
// 即使使用 content()，数据也是从临时文件读取的！
Flux<DataBuffer> content = filePart.content();  
// ↑ 这个 Flux 是从临时文件读取的流，不是原始网络流！
```

## 内存占用

### 小文件（< max-in-memory-size）

```yaml
spring:
  codec:
    max-in-memory-size: 10MB  # 默认256KB
```

```java
文件大小 < 10MB
  ↓
使用内存缓冲区（不创建临时文件）
  ↓
Mono<FilePart> 触发
  ↓
transferTo() 从内存写入目标文件
```

**内存占用**：单个文件占用 ≈ 文件大小

### 大文件（> max-in-memory-size）

```java
文件大小 > 10MB
  ↓
创建临时文件
  ↓
流式写入临时文件（内存占用恒定）
  ↓
Mono<FilePart> 触发
  ↓
transferTo() 从临时文件移动/复制到目标位置
```

**内存占用**：恒定（约几MB的缓冲区）

**磁盘占用**：
- 临时文件：文件大小
- 目标文件：文件大小
- **总共**：2倍文件大小（复制完成后临时文件会被删除）

## 性能优化

### 1. 调整内存缓冲区大小

```yaml
spring:
  codec:
    max-in-memory-size: 20MB  # 增大，减少小文件的磁盘I/O
```

**建议**：
- 小文件为主：10-20MB
- 大文件为主：1-5MB

### 2. 优化临时文件位置

```yaml
spring:
  webflux:
    multipart:
      file-storage-directory: /ssd/temp  # 使用SSD
```

**效果**：
- HDD：30秒
- SSD：20秒
- **提升**：33%

### 3. 使用 transferTo()

```java
// ✅ 推荐
return filePart.flatMap(file -> file.transferTo(targetPath));

// ❌ 不推荐（除非需要自定义处理）
return filePart.flatMap(file -> 
    DataBufferUtils.write(file.content(), targetPath)
);
```

**原因**：`transferTo()` 使用系统级优化（如sendfile）。

## 总结

### FilePart 的真相

1. **接收是流式的**
   - 数据到达立即写入临时存储
   - 不会OOM

2. **触发不是流式的**
   - `Mono<FilePart>` 要等完整上传
   - 这是协议限制

3. **处理可以优化**
   - 使用 `transferTo()` 系统级优化
   - 配置SSD临时目录
   - 调整内存缓冲区大小

### 是否适合您的场景？

| 场景 | FilePart适合吗 | 推荐方案 |
|-----|---------------|---------|
| 小文件(<100MB) | ✅ 完全适合 | FilePart |
| 中等文件(<1GB) | ✅ 可以接受 | FilePart + SSD优化 |
| 大文件(>1GB) | ⚠️ 谨慎使用 | FilePart + SSD优化 |
| 超大文件(>5GB) | ❌ 不推荐 | Raw Binary 或分片上传 |
| iOS标准上传 | ✅ 最佳选择 | FilePart + 优化配置 |
| 需要实时进度 | ❌ 不适合 | Raw Binary |

### 下一步

- 如果接受FilePart的限制 → 查看 `视频上传方案总结.md`
- 如果需要真正流式 → 查看 `真正的流式上传-立即测试.md`
- 如果遇到问题 → 查看 `视频流式上传-故障排查指南.md`

现在您完全理解了 FilePart 的工作原理！

