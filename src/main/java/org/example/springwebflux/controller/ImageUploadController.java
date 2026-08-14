package org.example.springwebflux.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 图片上传Controller (Spring WebFlux版本)
 * 用于接收HTTP POST multipart/form-data格式的图片数据并保存到文件系统
 * 使用响应式编程模型处理文件上传
 */
@RestController
@RequestMapping("/image")
public class ImageUploadController {

    private static final Logger logger = LoggerFactory.getLogger(ImageUploadController.class);
    
    // 允许上传的图片格式
    private static final List<String> ALLOWED_IMAGE_EXTENSIONS = Arrays.asList("jpg", "jpeg", "png", "gif", "bmp", "webp", "heic", "heif");
    
    // 允许上传的视频格式
    private static final List<String> ALLOWED_VIDEO_EXTENSIONS = Arrays.asList("mov", "mp4", "avi", "mkv");
    
    // 图片Content-Type映射
    private static final List<String> ALLOWED_IMAGE_CONTENT_TYPES = Arrays.asList(
        "image/jpeg", "image/jpg", "image/png", "image/gif", "image/bmp", 
        "image/webp", "image/heic", "image/heif", "image/heic-sequence", "image/heif-sequence"
    );
    
    // 视频Content-Type映射
    private static final List<String> ALLOWED_VIDEO_CONTENT_TYPES = Arrays.asList(
        "video/quicktime", "video/mp4", "video/x-msvideo", "video/x-matroska"
    );
    
    /**
     * 获取图片上传目录（动态读取，支持运行时更改）
     */
    private String getUploadDir() {
        String userHome = System.getProperty("user.home");
        String appName = "FileUploadManager";
        String defaultUploadBase = userHome + "/" + appName + "/uploads/";
        String uploadBaseDir = System.getProperty("app.upload.base.dir", defaultUploadBase);
        return System.getProperty("app.upload.image.dir", uploadBaseDir + "images/");
    }
    
    /**
     * 获取视频上传目录（动态读取，支持运行时更改）
     */
    private String getVideoDir() {
        String userHome = System.getProperty("user.home");
        String appName = "FileUploadManager";
        String defaultUploadBase = userHome + "/" + appName + "/uploads/";
        String uploadBaseDir = System.getProperty("app.upload.base.dir", defaultUploadBase);
        return System.getProperty("app.upload.video.dir", uploadBaseDir + "videos/");
    }
    
    /**
     * 获取Live Photo上传目录（动态读取，支持运行时更改）
     */
    private String getLivePhotoDir() {
        String userHome = System.getProperty("user.home");
        String appName = "FileUploadManager";
        String defaultUploadBase = userHome + "/" + appName + "/uploads/";
        String uploadBaseDir = System.getProperty("app.upload.base.dir", defaultUploadBase);
        return System.getProperty("app.upload.livephoto.dir", uploadBaseDir + "livephotos/");
    }
    
    // 静态初始化块：打印初始配置信息
    static {
        Logger initLogger = LoggerFactory.getLogger(ImageUploadController.class);
        initLogger.info("========================================");
        initLogger.info("ImageUploadController (WebFlux版本) 已加载");
        initLogger.info("路径将从系统属性动态读取，支持运行时更改");
        initLogger.info("========================================");
    }

    /**
     * 上传单个图片 (WebFlux版本)
     * 
     * @param filePart 上传的图片文件
     * @return 包含上传结果的Mono<ResponseEntity>
     */
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<ResponseEntity<Map<String, Object>>> uploadImage(@RequestPart("file") Mono<FilePart> filePart) {
        return filePart.flatMap(file -> {
            Map<String, Object> result = new HashMap<>();

            // 获取原始文件名
            String originalFilename = file.filename();
            logger.info("接收到上传文件: {}, Content-Type: {}", originalFilename, 
                       file.headers().getContentType());

            // 验证文件格式（先验证扩展名，失败则验证Content-Type）
            String fileExtension = getFileExtension(originalFilename);
            String contentType = file.headers().getContentType() != null ? 
                               file.headers().getContentType().toString() : "";
            
            if (!isValidImageExtension(fileExtension)) {
                // 扩展名验证失败，尝试通过Content-Type验证
                if (!isValidImageContentType(contentType)) {
                    logger.warn("不支持的文件格式 - 扩展名: {}, Content-Type: {}", fileExtension, contentType);
                    result.put("success", false);
                    result.put("message", "不支持的图片格式");
                    return Mono.just(ResponseEntity.badRequest().body(result));
                }
                logger.info("扩展名为空或未知，但Content-Type验证通过: {}", contentType);
                
                // 从Content-Type获取扩展名并拼接到文件名
                String extensionFromContentType = getExtensionFromContentType(contentType);
                if (!extensionFromContentType.isEmpty()) {
                    originalFilename = originalFilename + "." + extensionFromContentType;
                    logger.info("文件名添加扩展名: {}", originalFilename);
                }
            }

            // 在阻塞调度器上执行文件系统操作
            return saveFile(file, originalFilename, getUploadDir(), result, "image")
                    .subscribeOn(Schedulers.boundedElastic());
        }).onErrorResume(e -> {
            logger.error("上传过程发生异常", e);
            Map<String, Object> result = new HashMap<>();
            result.put("success", false);
            result.put("message", "上传失败: " + e.getMessage());
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result));
        });
    }

    /**
     * 批量上传多个图片 (WebFlux版本)
     * 
     * @param filePartFlux 上传的图片文件流
     * @return 包含上传结果的Mono<ResponseEntity>
     */
    @PostMapping(value = "/batch-upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<ResponseEntity<Map<String, Object>>> batchUploadImages(
            @RequestPart("files") Flux<FilePart> filePartFlux) {
        
        logger.info("接收到批量上传请求");

        return filePartFlux
                .index()
                .flatMap(tuple -> {
                    Long index = tuple.getT1();
                    FilePart file = tuple.getT2();
                    
                    Map<String, Object> fileResult = new HashMap<>();
                    String originalFilename = file.filename();
                    fileResult.put("originalFilename", originalFilename);

                    String fileExtension = getFileExtension(originalFilename);
                    if (!isValidImageExtension(fileExtension)) {
                        fileResult.put("success", false);
                        fileResult.put("message", "不支持的图片格式");
                        return Mono.just(fileResult);
                    }

                    String newFilename = generateFilename(fileExtension);
                    String datePath = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
                    String uploadPath = getUploadDir() + datePath + "/";

                    return Mono.fromCallable(() -> {
                        File uploadDir = new File(uploadPath);
                        if (!uploadDir.exists()) {
                            uploadDir.mkdirs();
                        }
                        return uploadPath;
                    }).subscribeOn(Schedulers.boundedElastic())
                    .flatMap(path -> {
                        String filePath = path + newFilename;
                        Path targetPath = Paths.get(filePath);
                        
                        return DataBufferUtils.write(file.content(), targetPath, StandardOpenOption.CREATE)
                                .then(Mono.fromCallable(() -> {
                                    long fileSize = Files.size(targetPath);
                                    fileResult.put("success", true);
                                    fileResult.put("savedFilename", newFilename);
                                    fileResult.put("filePath", filePath);
                                    fileResult.put("fileSize", fileSize);
                                    logger.info("批量上传-文件保存成功: {}", filePath);
                                    return fileResult;
                                }).subscribeOn(Schedulers.boundedElastic()));
                    }).onErrorResume(e -> {
                        logger.error("批量上传-文件保存失败: " + originalFilename, e);
                        fileResult.put("success", false);
                        fileResult.put("message", "保存失败: " + e.getMessage());
                        return Mono.just(fileResult);
                    });
                })
                .collectList()
                .map(fileResults -> {
                    Map<String, Object> result = new HashMap<>();
                    List<Map<String, Object>> successList = fileResults.stream()
                            .filter(r -> Boolean.TRUE.equals(r.get("success")))
                            .collect(Collectors.toList());
                    List<Map<String, Object>> failList = fileResults.stream()
                            .filter(r -> !Boolean.TRUE.equals(r.get("success")))
                            .collect(Collectors.toList());

                    result.put("total", fileResults.size());
                    result.put("successCount", successList.size());
                    result.put("failCount", failList.size());
                    result.put("successList", successList);
                    result.put("failList", failList);

                    logger.info("批量上传完成，成功: {}, 失败: {}", successList.size(), failList.size());
                    return ResponseEntity.ok(result);
                });
    }

    /**
     * 检查文件是否已存在 (WebFlux版本)
     * 接收iOS端发送的JSON请求，根据文件名和文件类型检查文件是否已存在
     * 
     * @param requestBody 包含fileName和fileType的请求体
     * @return 包含exists字段的Mono<ResponseEntity>
     */
    @PostMapping("/check-exists")
    public Mono<ResponseEntity<Map<String, Object>>> checkFileExists(
            @RequestBody Map<String, String> requestBody) {
        
        return Mono.fromCallable(() -> {
            Map<String, Object> result = new HashMap<>();

            // 获取请求参数
            String fileName = requestBody.get("fileName");
            String fileType = requestBody.get("fileType");
            
            logger.info("收到文件存在性检查请求 - 文件名: {}, 文件类型: {}", fileName, fileType);

            // 参数验证
            if (fileName == null || fileName.isEmpty()) {
                logger.warn("文件名为空");
                result.put("exists", false);
                result.put("message", "文件名不能为空");
                return ResponseEntity.badRequest().body(result);
            }

            if (fileType == null || fileType.isEmpty()) {
                logger.warn("文件类型为空");
                result.put("exists", false);
                result.put("message", "文件类型不能为空");
                return ResponseEntity.badRequest().body(result);
            }

            // 提取文件名前缀（去掉扩展名）
            final String fileNamePrefix;
            int dotIndex = fileName.lastIndexOf('.');
            if (dotIndex > 0) {
                fileNamePrefix = fileName.substring(0, dotIndex);
            } else {
                fileNamePrefix = fileName;
            }
            logger.info("提取文件名前缀: {} -> {}", fileName, fileNamePrefix);

            // 根据文件类型确定检查路径
            boolean exists = false;
            String matchedFileName = "";
            
            switch (fileType.toLowerCase()) {
                case "image":
                    // 检查图片目录，查找以该前缀开头的文件
                    File imageDir = new File(getUploadDir());
                    if (imageDir.exists() && imageDir.isDirectory()) {
                        File[] imageFiles = imageDir.listFiles((dir, name) -> {
                            String namePrefix = name;
                            int idx = name.lastIndexOf('.');
                            if (idx > 0) {
                                namePrefix = name.substring(0, idx);
                            }
                            return namePrefix.equals(fileNamePrefix);
                        });
                        
                        if (imageFiles != null && imageFiles.length > 0) {
                            exists = true;
                            matchedFileName = imageFiles[0].getName();
                            logger.info("找到匹配的图片文件: {}", matchedFileName);
                        } else {
                            logger.info("未找到前缀为 {} 的图片文件", fileNamePrefix);
                        }
                    }
                    break;
                    
                case "video":
                    // 检查视频目录，查找以该前缀开头的文件
                    File videoDir = new File(getVideoDir());
                    if (videoDir.exists() && videoDir.isDirectory()) {
                        File[] videoFiles = videoDir.listFiles((dir, name) -> {
                            String namePrefix = name;
                            int idx = name.lastIndexOf('.');
                            if (idx > 0) {
                                namePrefix = name.substring(0, idx);
                            }
                            return namePrefix.equals(fileNamePrefix);
                        });
                        
                        if (videoFiles != null && videoFiles.length > 0) {
                            exists = true;
                            matchedFileName = videoFiles[0].getName();
                            logger.info("找到匹配的视频文件: {}", matchedFileName);
                        } else {
                            logger.info("未找到前缀为 {} 的视频文件", fileNamePrefix);
                        }
                    }
                    break;
                    
                case "livephoto":
                    // 检查Live Photo目录（需要检查图片和视频是否都存在）
                    File livePhotoDir = new File(getLivePhotoDir());
                    if (livePhotoDir.exists() && livePhotoDir.isDirectory()) {
                        // 查找图片文件（heic, heif, jpg, png等）
                        File[] imageFiles = livePhotoDir.listFiles((dir, name) -> {
                            String namePrefix = name;
                            int idx = name.lastIndexOf('.');
                            if (idx > 0) {
                                namePrefix = name.substring(0, idx);
                            }
                            String ext = getFileExtension(name).toLowerCase();
                            return namePrefix.equals(fileNamePrefix) && isValidImageExtension(ext);
                        });
                        
                        // 查找视频文件（mov, mp4等）
                        File[] videoFiles = livePhotoDir.listFiles((dir, name) -> {
                            String namePrefix = name;
                            int idx = name.lastIndexOf('.');
                            if (idx > 0) {
                                namePrefix = name.substring(0, idx);
                            }
                            String ext = getFileExtension(name).toLowerCase();
                            return namePrefix.equals(fileNamePrefix) && isValidVideoExtension(ext);
                        });
                        
                        boolean imageExists = imageFiles != null && imageFiles.length > 0;
                        boolean videoExists = videoFiles != null && videoFiles.length > 0;
                        
                        // Live Photo需要图片和视频都存在
                        exists = imageExists && videoExists;
                        
                        if (exists) {
                            matchedFileName = imageFiles[0].getName() + " + " + videoFiles[0].getName();
                            logger.info("找到匹配的Live Photo: 图片={}, 视频={}", 
                                       imageFiles[0].getName(), videoFiles[0].getName());
                        } else {
                            logger.info("未找到完整的Live Photo: 图片={}, 视频={}", imageExists, videoExists);
                        }
                    }
                    break;
                    
                default:
                    logger.warn("不支持的文件类型: {}", fileType);
                    result.put("exists", false);
                    result.put("message", "不支持的文件类型: " + fileType);
                    return ResponseEntity.badRequest().body(result);
            }

            // 返回结果
            result.put("exists", exists);
            result.put("fileName", fileName);
            result.put("fileType", fileType);
            
            if (exists) {
                result.put("message", "文件已存在");
                result.put("matchedFileName", matchedFileName);
            } else {
                result.put("message", "文件不存在");
            }

            logger.info("文件检查完成 - 文件名: {}, 类型: {}, 结果: {}, 匹配文件: {}", 
                       fileName, fileType, exists ? "存在" : "不存在", matchedFileName);
            return ResponseEntity.ok(result);
            
        }).subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            logger.error("检查文件存在性时发生异常", e);
            Map<String, Object> result = new HashMap<>();
            result.put("exists", false);
            result.put("message", "检查失败: " + e.getMessage());
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result));
        });
    }

    /**
     * 上传单个视频文件 (WebFlux版本)
     * 接收iOS端发送的multipart/form-data视频数据
     * 
     * @param filePart 视频文件（name="file"）
     * @return 包含上传结果的Mono<ResponseEntity>
     */
    @PostMapping(value = "/upload-video", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<ResponseEntity<Map<String, Object>>> uploadVideo(@RequestPart("file") Mono<FilePart> filePart) {
        return filePart.flatMap(file -> {
            Map<String, Object> result = new HashMap<>();

            // 获取原始文件名
            String originalFilename = file.filename();
            String contentType = file.headers().getContentType() != null ? 
                               file.headers().getContentType().toString() : "";
            logger.info("接收到视频上传: {}, Content-Type: {}", originalFilename, contentType);

            // 验证文件格式（先验证扩展名，失败则验证Content-Type）
            String fileExtension = getFileExtension(originalFilename);
            if (!isValidVideoExtension(fileExtension)) {
                // 扩展名验证失败，尝试通过Content-Type验证
                if (!isValidVideoContentType(contentType)) {
                    logger.warn("不支持的视频格式 - 扩展名: {}, Content-Type: {}", fileExtension, contentType);
                    result.put("success", false);
                    result.put("message", "不支持的视频格式");
                    return Mono.just(ResponseEntity.badRequest().body(result));
                }
                logger.info("扩展名为空或未知，但Content-Type验证通过: {}", contentType);
                
                // 从Content-Type获取扩展名并拼接到文件名
                String extensionFromContentType = getExtensionFromContentType(contentType);
                if (!extensionFromContentType.isEmpty()) {
                    originalFilename = originalFilename + "." + extensionFromContentType;
                    fileExtension = extensionFromContentType;
                    logger.info("视频文件名添加扩展名: {}", originalFilename);
                }
            }

            // 在阻塞调度器上执行文件系统操作
            String finalFilename = originalFilename;
            String finalExtension = fileExtension;
            return saveFile(file, finalFilename, getVideoDir(), result, "video")
                    .map(response -> {
                        // 添加视频特有的字段
                        Map<String, Object> body = response.getBody();
                        if (body != null) {
                            body.put("fileType", "video");
                            body.put("extension", finalExtension);
                        }
                        return response;
                    })
                    .subscribeOn(Schedulers.boundedElastic());
        }).onErrorResume(e -> {
            logger.error("视频上传过程发生异常", e);
            Map<String, Object> result = new HashMap<>();
            result.put("success", false);
            result.put("message", "视频上传失败: " + e.getMessage());
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result));
        });
    }

    /**
     * 上传Live Photo（包含图片和视频） (WebFlux版本)
     * 接收iOS端发送的multipart/form-data，包含：
     * - image: livephoto.heic (图片文件)
     * - video: livephoto.mov (视频文件)
     * - type: livephoto (可选的类型标识)
     * 
     * @param imageFilePart 图片文件（name="image"）
     * @param videoFilePart 视频文件（name="video"）
     * @param type 类型标识（name="type"，可选）
     * @return 包含上传结果的Mono<ResponseEntity>
     */
    @PostMapping(value = "/upload-livephoto", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<ResponseEntity<Map<String, Object>>> uploadLivePhoto(
            @RequestPart(value = "image", required = false) Mono<FilePart> imageFilePart,
            @RequestPart(value = "video", required = false) Mono<FilePart> videoFilePart,
            @RequestPart(value = "type", required = false) String type) {
        
        // 使用zip组合两个Mono
        return Mono.zip(
                imageFilePart.switchIfEmpty(Mono.error(new IllegalArgumentException("Live Photo 图片文件不能为空"))),
                videoFilePart.switchIfEmpty(Mono.error(new IllegalArgumentException("Live Photo 视频文件不能为空")))
        ).flatMap(tuple -> {
            FilePart imageFile = tuple.getT1();
            FilePart videoFile = tuple.getT2();
            
            Map<String, Object> result = new HashMap<>();

            // 获取原始文件名和Content-Type
            String imageOriginalFilename = imageFile.filename();
            String videoOriginalFilename = videoFile.filename();
            String imageContentType = imageFile.headers().getContentType() != null ? 
                                     imageFile.headers().getContentType().toString() : "";
            String videoContentType = videoFile.headers().getContentType() != null ? 
                                     videoFile.headers().getContentType().toString() : "";
            
            // 检查文件名是否为空
            if (imageOriginalFilename == null || imageOriginalFilename.isEmpty()) {
                logger.warn("Live Photo 图片文件名为空");
                result.put("success", false);
                result.put("message", "Live Photo 图片文件名不能为空");
                return Mono.just(ResponseEntity.badRequest().body(result));
            }
            
            if (videoOriginalFilename == null || videoOriginalFilename.isEmpty()) {
                logger.warn("Live Photo 视频文件名为空");
                result.put("success", false);
                result.put("message", "Live Photo 视频文件名不能为空");
                return Mono.just(ResponseEntity.badRequest().body(result));
            }
            
            logger.info("接收到Live Photo上传请求");
            logger.info("  图片: {}, Content-Type: {}", imageOriginalFilename, imageContentType);
            logger.info("  视频: {}, Content-Type: {}", videoOriginalFilename, videoContentType);
            logger.info("  类型: {}", type != null ? type : "未指定");

            // 验证图片文件格式（先验证扩展名，失败则验证Content-Type）
            String imageExtension = getFileExtension(imageOriginalFilename);
            if (!isValidImageExtension(imageExtension)) {
                // 扩展名验证失败，尝试通过Content-Type验证
                if (!isValidImageContentType(imageContentType)) {
                    logger.warn("不支持的图片格式 - 扩展名: {}, Content-Type: {}", imageExtension, imageContentType);
                    result.put("success", false);
                    result.put("message", "不支持的图片格式");
                    return Mono.just(ResponseEntity.badRequest().body(result));
                }
                logger.info("图片扩展名为空或未知，但Content-Type验证通过: {}", imageContentType);
                
                // 从Content-Type获取扩展名并拼接到文件名
                String extensionFromContentType = getExtensionFromContentType(imageContentType);
                if (!extensionFromContentType.isEmpty()) {
                    imageOriginalFilename = imageOriginalFilename + "." + extensionFromContentType;
                    imageExtension = extensionFromContentType;
                    logger.info("图片文件名添加扩展名: {}", imageOriginalFilename);
                }
            }

            // 验证视频文件格式（先验证扩展名，失败则验证Content-Type）
            String videoExtension = getFileExtension(videoOriginalFilename);
            if (!isValidVideoExtension(videoExtension)) {
                // 扩展名验证失败，尝试通过Content-Type验证
                if (!isValidVideoContentType(videoContentType)) {
                    logger.warn("不支持的视频格式 - 扩展名: {}, Content-Type: {}", videoExtension, videoContentType);
                    result.put("success", false);
                    result.put("message", "不支持的视频格式");
                    return Mono.just(ResponseEntity.badRequest().body(result));
                }
                logger.info("视频扩展名为空或未知，但Content-Type验证通过: {}", videoContentType);
                
                // 从Content-Type获取扩展名并拼接到文件名
                String extensionFromContentType = getExtensionFromContentType(videoContentType);
                if (!extensionFromContentType.isEmpty()) {
                    videoOriginalFilename = videoOriginalFilename + "." + extensionFromContentType;
                    videoExtension = extensionFromContentType;
                    logger.info("视频文件名添加扩展名: {}", videoOriginalFilename);
                }
            }

            // 保存两个文件
            String livePhotoDirPath = getLivePhotoDir();
            String finalImageFilename = imageOriginalFilename;
            String finalVideoFilename = videoOriginalFilename;
            
            return Mono.fromCallable(() -> {
                // 确保目录存在
                File uploadDir = new File(livePhotoDirPath);
                if (!uploadDir.exists()) {
                    boolean created = uploadDir.mkdirs();
                    if (created) {
                        logger.info("创建Live Photo上传目录成功: {}", uploadDir.getAbsolutePath());
                    } else {
                        throw new RuntimeException("创建上传目录失败，请检查应用权限");
                    }
                } else {
                    logger.debug("Live Photo上传目录已存在: {}", uploadDir.getAbsolutePath());
                }
                return livePhotoDirPath;
            }).subscribeOn(Schedulers.boundedElastic())
            .flatMap(dirPath -> {
                // 构建文件路径
                String imageFilePath = dirPath + finalImageFilename;
                String videoFilePath = dirPath + finalVideoFilename;
                File imageTargetFile = new File(imageFilePath);
                File videoTargetFile = new File(videoFilePath);
                
                // 检查Live Photo是否已存在（两个文件都存在）
                if (imageTargetFile.exists() && videoTargetFile.exists()) {
                    logger.info("Live Photo 已存在，跳过保存 - 图片: {}, 视频: {}", finalImageFilename, finalVideoFilename);
                    
                    result.put("success", true);
                    result.put("message", "Live Photo 已存在，无需重复上传");
                    result.put("existed", true);
                    
                    Map<String, Object> imageResult = new HashMap<>();
                    imageResult.put("filename", finalImageFilename);
                    imageResult.put("filePath", imageFilePath);
                    result.put("image", imageResult);
                    
                    Map<String, Object> videoResult = new HashMap<>();
                    videoResult.put("filename", finalVideoFilename);
                    videoResult.put("filePath", videoFilePath);
                    result.put("video", videoResult);
                    
                    return Mono.just(ResponseEntity.ok(result));
                }
                
                // 保存图片和视频文件
                Path imagePath = Paths.get(imageFilePath);
                Path videoPath = Paths.get(videoFilePath);
                
                Mono<Void> saveImageMono = DataBufferUtils.write(
                    imageFile.content(), 
                    imagePath, 
                    StandardOpenOption.CREATE
                );
                
                Mono<Void> saveVideoMono = DataBufferUtils.write(
                    videoFile.content(), 
                    videoPath, 
                    StandardOpenOption.CREATE
                );
                
                return Mono.when(saveImageMono, saveVideoMono)
                        .then(Mono.fromCallable(() -> {
                            long imageSize = Files.size(imagePath);
                            long videoSize = Files.size(videoPath);
                            
                            logger.info("Live Photo 图片保存成功: {}", imageFilePath);
                            logger.info("Live Photo 视频保存成功: {}", videoFilePath);
                            
                            result.put("success", true);
                            result.put("message", "Live Photo 上传成功");
                            result.put("existed", false);
                            
                            Map<String, Object> imageResult = new HashMap<>();
                            imageResult.put("filename", finalImageFilename);
                            imageResult.put("filePath", imageFilePath);
                            imageResult.put("size", imageSize);
                            imageResult.put("contentType", imageContentType);
                            result.put("image", imageResult);
                            
                            Map<String, Object> videoResult = new HashMap<>();
                            videoResult.put("filename", finalVideoFilename);
                            videoResult.put("filePath", videoFilePath);
                            videoResult.put("size", videoSize);
                            videoResult.put("contentType", videoContentType);
                            result.put("video", videoResult);

                            logger.info("Live Photo 上传完成 - 图片: {}, 视频: {}", finalImageFilename, finalVideoFilename);
                            
                            return ResponseEntity.ok(result);
                        }).subscribeOn(Schedulers.boundedElastic()));
            });
        }).onErrorResume(e -> {
            logger.error("Live Photo 上传过程发生异常", e);
            Map<String, Object> result = new HashMap<>();
            result.put("success", false);
            result.put("message", "上传失败: " + e.getMessage());
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result));
        });
    }

    /**
     * 获取服务器计算机名称 (WebFlux版本)
     * 
     * @return 包含计算机名称、IP地址等信息的Mono<ResponseEntity>
     */
    @GetMapping("/server-info")
    public Mono<ResponseEntity<Map<String, Object>>> getServerInfo() {
        return Mono.fromCallable(() -> {
            Map<String, Object> result = new HashMap<>();
            
            try {
                // 获取本机信息
                InetAddress localHost = InetAddress.getLocalHost();
                
                // 计算机名称
                String hostName = localHost.getHostName();
                
                // IP地址
                String ipAddress = localHost.getHostAddress();
                
                // 获取系统属性
                String osName = System.getProperty("os.name");
                String osVersion = System.getProperty("os.version");
                String osArch = System.getProperty("os.arch");
                String userName = System.getProperty("user.name");
                String userHome = System.getProperty("user.home");
                
                logger.info("获取服务器信息 - 计算机名: {}, IP: {}, 操作系统: {}", hostName, ipAddress, osName);
                
                // 组装返回结果
                result.put("success", true);
                result.put("computerName", hostName);
                result.put("hostName", hostName);
                result.put("ipAddress", ipAddress);
                result.put("osName", osName);
                result.put("osVersion", osVersion);
                result.put("osArch", osArch);
                result.put("userName", userName);
                result.put("userHome", userHome);
                result.put("timestamp", System.currentTimeMillis());
                
                return ResponseEntity.ok(result);
                
            } catch (UnknownHostException e) {
                logger.error("无法获取服务器信息", e);
                result.put("success", false);
                result.put("message", "无法获取服务器信息: " + e.getMessage());
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
        }).subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            logger.error("获取服务器信息时发生异常", e);
            Map<String, Object> result = new HashMap<>();
            result.put("success", false);
            result.put("message", "获取失败: " + e.getMessage());
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result));
        });
    }

    // ==================== 辅助方法 ====================

    /**
     * 保存文件的通用方法 (响应式版本)
     */
    private Mono<ResponseEntity<Map<String, Object>>> saveFile(
            FilePart file, String filename, String uploadDir, 
            Map<String, Object> result, String fileType) {
        
        return Mono.fromCallable(() -> {
            // 确保目录存在
            File dir = new File(uploadDir);
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                if (created) {
                    logger.info("创建{}上传目录成功: {}", fileType, dir.getAbsolutePath());
                } else {
                    throw new RuntimeException("创建上传目录失败，请检查应用权限");
                }
            }
            return uploadDir;
        }).subscribeOn(Schedulers.boundedElastic())
        .flatMap(dirPath -> {
            String filePath = dirPath + filename;
            File targetFile = new File(filePath);
            
            // 确保父目录存在（双重保险）
            File parentDir = targetFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }
            
            // 检查文件是否已存在
            if (targetFile.exists()) {
                logger.info("{}文件已存在，跳过保存: {}", fileType, filePath);
                result.put("success", true);
                result.put("message", fileType + "文件已存在，无需重复上传");
                result.put("originalFilename", filename);
                result.put("savedFilename", filename);
                result.put("filePath", filePath);
                result.put("contentType", file.headers().getContentType() != null ? 
                          file.headers().getContentType().toString() : "");
                result.put("existed", true);
                return Mono.just(ResponseEntity.ok(result));
            }
            
            // 保存文件
            Path path = Paths.get(filePath);
            return DataBufferUtils.write(file.content(), path, StandardOpenOption.CREATE)
                    .then(Mono.fromCallable(() -> {
                        long fileSize = Files.size(path);
                        logger.info("{}文件保存成功: {}", fileType, filePath);
                        
                        result.put("success", true);
                        result.put("message", fileType + "上传成功");
                        result.put("originalFilename", filename);
                        result.put("savedFilename", filename);
                        result.put("filePath", filePath);
                        result.put("fileSize", fileSize);
                        result.put("contentType", file.headers().getContentType() != null ? 
                                  file.headers().getContentType().toString() : "");
                        result.put("existed", false);
                        
                        return ResponseEntity.ok(result);
                    }).subscribeOn(Schedulers.boundedElastic()));
        });
    }

    /**
     * 获取文件扩展名
     */
    private String getFileExtension(String filename) {
        if (filename == null || filename.isEmpty()) {
            return "";
        }
        int lastDotIndex = filename.lastIndexOf('.');
        if (lastDotIndex > 0 && lastDotIndex < filename.length() - 1) {
            return filename.substring(lastDotIndex + 1).toLowerCase();
        }
        return "";
    }

    /**
     * 验证是否为支持的图片格式（通过扩展名）
     */
    private boolean isValidImageExtension(String extension) {
        if (extension == null || extension.isEmpty()) {
            return false;
        }
        return ALLOWED_IMAGE_EXTENSIONS.contains(extension.toLowerCase());
    }
    
    /**
     * 验证是否为支持的视频格式（通过扩展名）
     */
    private boolean isValidVideoExtension(String extension) {
        if (extension == null || extension.isEmpty()) {
            return false;
        }
        return ALLOWED_VIDEO_EXTENSIONS.contains(extension.toLowerCase());
    }
    
    /**
     * 验证是否为支持的图片格式（通过Content-Type）
     */
    private boolean isValidImageContentType(String contentType) {
        if (contentType == null || contentType.isEmpty()) {
            return false;
        }
        // Content-Type可能包含额外的参数，如 "image/png; charset=utf-8"
        String baseContentType = contentType.split(";")[0].trim().toLowerCase();
        return ALLOWED_IMAGE_CONTENT_TYPES.contains(baseContentType);
    }
    
    /**
     * 验证是否为支持的视频格式（通过Content-Type）
     */
    private boolean isValidVideoContentType(String contentType) {
        if (contentType == null || contentType.isEmpty()) {
            return false;
        }
        // Content-Type可能包含额外的参数
        String baseContentType = contentType.split(";")[0].trim().toLowerCase();
        return ALLOWED_VIDEO_CONTENT_TYPES.contains(baseContentType);
    }

    /**
     * 从Content-Type获取文件扩展名
     * 
     * @param contentType Content-Type字符串
     * @return 对应的文件扩展名，如果无法识别则返回空字符串
     */
    private String getExtensionFromContentType(String contentType) {
        if (contentType == null || contentType.isEmpty()) {
            return "";
        }
        
        // 去除Content-Type中的参数
        String baseContentType = contentType.split(";")[0].trim().toLowerCase();
        
        // 图片类型映射
        switch (baseContentType) {
            case "image/jpeg":
            case "image/jpg":
                return "jpg";
            case "image/png":
                return "png";
            case "image/gif":
                return "gif";
            case "image/bmp":
                return "bmp";
            case "image/webp":
                return "webp";
            case "image/heic":
            case "image/heic-sequence":
                return "heic";
            case "image/heif":
            case "image/heif-sequence":
                return "heif";
            // 视频类型映射
            case "video/quicktime":
                return "mov";
            case "video/mp4":
                return "mp4";
            case "video/x-msvideo":
                return "avi";
            case "video/x-matroska":
                return "mkv";
            default:
                return "";
        }
    }

    /**
     * 生成唯一的文件名
     */
    private String generateFilename(String extension) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String timestamp = String.valueOf(System.currentTimeMillis());
        return uuid + "_" + timestamp + "." + extension;
    }
}

