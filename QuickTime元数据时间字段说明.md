# QuickTime 元数据时间字段说明

## 问题背景

iOS端显示的视频元数据和服务端提取的时间不一致：
- iOS端: `2025-11-22T13:36:37+0800`
- 服务端: `2025-11-22T23:28:40+0800`

## QuickTime 时间字段详解

QuickTime/MOV 视频文件包含多个时间字段，metadata-extractor库支持的主要字段：

### 主要时间字段

| 字段常量 | 说明 | 可能的值 |
|---------|------|---------|
| `TAG_CREATION_TIME` | 文件创建时间 | 可能是导出/编码时间，不是拍摄时间 |
| `TAG_MODIFICATION_TIME` | 文件修改时间 | 最后一次修改的时间 |
| `TAG_MEDIA_TIME_SCALE` | 媒体时间刻度 | 不是时间戳 |

### iOS视频的特殊情况

当iOS导出视频到临时文件准备上传时：
```
原始视频拍摄时间: 2025-11-22 13:36:37
   ↓
iOS从相册读取视频
   ↓
导出到临时文件（可能重新编码）
   ↓
QuickTime Creation Time: 2025-11-22 23:28:40 ← 导出时间！
```

**结论**: `TAG_CREATION_TIME` 可能是导出时间，不是拍摄时间。

## metadata-extractor 可用的QuickTime标签

查看 QuickTime 元数据的所有可用字段：

```java
QuickTimeDirectory quickTime = metadata.getFirstDirectoryOfType(QuickTimeDirectory.class);
if (quickTime != null) {
    // 打印所有标签
    for (Tag tag : quickTime.getTags()) {
        logger.info("QuickTime 标签: {} = {}", tag.getTagName(), tag.getDescription());
    }
}
```

### 常见QuickTime标签

```
Creation Time - 文件创建时间
Modification Time - 文件修改时间
Duration - 视频时长
Preferred Rate - 播放速率
Preferred Volume - 音量
Preview Time - 预览时间
Preview Duration - 预览时长
Poster Time - 海报时间
Selection Time - 选择时间
Current Time - 当前时间
Next Track ID - 下一个轨道ID
...
```

## 解决方案

### 方案1: 检查所有QuickTime时间字段

```java
private Long extractTimestampFromMetadata(MultipartFile file) {
    try (InputStream inputStream = new BufferedInputStream(file.getInputStream())) {
        Metadata metadata = ImageMetadataReader.readMetadata(inputStream);
        
        // 对于视频文件，尝试从 QuickTime 元数据读取
        QuickTimeDirectory quickTime = metadata.getFirstDirectoryOfType(QuickTimeDirectory.class);
        if (quickTime != null) {
            // 打印所有QuickTime标签用于调试
            logger.info("=== QuickTime 元数据 ===");
            for (Tag tag : quickTime.getTags()) {
                logger.info("  {} = {}", tag.getTagName(), tag.getDescription());
            }
            logger.info("========================");
            
            // 尝试多个时间字段
            Date creationTime = quickTime.getDate(QuickTimeDirectory.TAG_CREATION_TIME);
            Date modificationTime = quickTime.getDate(QuickTimeDirectory.TAG_MODIFICATION_TIME);
            
            // 优先使用修改时间（可能更接近拍摄时间）
            if (modificationTime != null) {
                logger.info("从 QuickTime Modification Time 提取: {} -> {}", 
                           file.getOriginalFilename(), modificationTime);
                return modificationTime.getTime();
            }
            
            // 其次使用创建时间
            if (creationTime != null) {
                logger.info("从 QuickTime Creation Time 提取: {} -> {}", 
                           file.getOriginalFilename(), creationTime);
                return creationTime.getTime();
            }
        }
        
        // ... 其他元数据源 ...
    }
}
```

### 方案2: 客户端传递timestamp参数（最可靠）⭐⭐⭐

**推荐方案**：让iOS端在上传时传递原始拍摄时间：

#### iOS端修改

```swift
// 获取视频资源
let asset = PHAsset.fetchAssets(...)

// 获取原始创建时间
if let creationDate = asset.creationDate {
    let timestamp = Int64(creationDate.timeIntervalSince1970 * 1000) // 转为毫秒
    
    // 添加到上传请求
    formData.append(
        "\(timestamp)".data(using: .utf8)!,
        withName: "timestamp"
    )
    
    print("📅 上传视频原始时间: \(creationDate), timestamp: \(timestamp)")
}
```

#### 服务端处理

服务端代码已经支持timestamp参数：

```java
@PostMapping("/upload-video")
public ResponseEntity<Map<String, Object>> uploadVideo(
        @RequestParam("file") MultipartFile file,
        @RequestParam(value = "timestamp", required = false) Long timestamp) {
    
    // 优先级：元数据 > 客户端参数 > 不设置
    Long finalTimestamp = null;
    
    // 1. 尝试从元数据提取
    Long metadataTimestamp = extractTimestampFromMetadata(file);
    if (metadataTimestamp != null) {
        finalTimestamp = metadataTimestamp;
    } 
    // 2. 使用客户端提供的timestamp
    else if (timestamp != null) {
        finalTimestamp = timestamp;  // ← iOS传递的正确时间
    }
    
    // 设置文件时间戳
    if (finalTimestamp != null) {
        setFileTimestamp(targetFile, finalTimestamp);
    }
}
```

### 方案3: 修改优先级，优先使用客户端时间

如果元数据不可靠，修改优先级：

```java
// 确定最终使用的时间戳（修改优先级：客户端 > 元数据）
Long finalTimestamp = null;
String timestampSource = null;

// 优先使用客户端提供的时间戳（iOS直接从相册获取，最准确）
if (timestamp != null && timestamp > 0) {
    finalTimestamp = timestamp;
    timestampSource = "client";
    logger.info("使用客户端提供的时间戳: {}", new Date(finalTimestamp));
} 
// 如果客户端没提供，尝试从元数据提取
else {
    Long metadataTimestamp = extractTimestampFromMetadata(file);
    if (metadataTimestamp != null && metadataTimestamp > 0) {
        finalTimestamp = metadataTimestamp;
        timestampSource = "metadata";
        logger.info("使用从元数据提取的时间戳: {}", new Date(finalTimestamp));
    }
}
```

## 完整的调试方案

### 步骤1: 打印所有QuickTime元数据

临时修改代码，打印所有QuickTime标签：

```java
QuickTimeDirectory quickTime = metadata.getFirstDirectoryOfType(QuickTimeDirectory.class);
if (quickTime != null) {
    logger.info("=== QuickTime 元数据详情 ===");
    logger.info("文件名: {}", file.getOriginalFilename());
    
    for (Tag tag : quickTime.getTags()) {
        logger.info("  [{}] {} = {}", 
                   tag.getTagType(), 
                   tag.getTagName(), 
                   tag.getDescription());
    }
    
    // 尝试获取所有可能的时间
    try {
        Date creation = quickTime.getDate(QuickTimeDirectory.TAG_CREATION_TIME);
        logger.info("  Creation Time: {}", creation);
    } catch (Exception e) {}
    
    try {
        Date modification = quickTime.getDate(QuickTimeDirectory.TAG_MODIFICATION_TIME);
        logger.info("  Modification Time: {}", modification);
    } catch (Exception e) {}
    
    logger.info("=============================");
}
```

### 步骤2: iOS端对比

iOS端打印所有元数据：

```swift
let asset = AVURLAsset(url: videoURL)

// 获取所有元数据
for item in asset.commonMetadata {
    print("QuickTime 元数据:")
    print("  key: \(item.commonKey?.rawValue ?? "nil")")
    print("  value: \(item.value ?? "nil")")
}

// 特别关注创建时间
if let creationDate = asset.creationDate {
    print("AVAsset CreationDate: \(creationDate)")
}
```

### 步骤3: 对比分析

对比两边的日志，找出：
1. iOS端的creationDate对应QuickTime的哪个字段
2. 为什么服务端提取的时间不同
3. 是否有其他字段包含正确的拍摄时间

## 总结

### 推荐方案（按优先级）

1. **最可靠**: iOS端传递 `timestamp` 参数（从 `PHAsset.creationDate` 获取）
2. **次优**: 打印QuickTime所有元数据，找到正确的时间字段
3. **备选**: 修改优先级，客户端时间 > 元数据时间

### 立即行动

#### iOS端修改

```swift
// 在上传时添加timestamp参数
let timestamp = Int64(asset.creationDate.timeIntervalSince1970 * 1000)
formData.append("\(timestamp)".data(using: .utf8)!, withName: "timestamp")
```

#### 服务端验证

```bash
# 查看日志，确认收到正确的timestamp
tail -f logs/application.log | grep "使用客户端提供的时间戳"
```

这样就能确保使用正确的视频拍摄时间！

