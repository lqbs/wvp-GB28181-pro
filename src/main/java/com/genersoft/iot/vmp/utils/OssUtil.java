package com.genersoft.iot.vmp.utils;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadFileRequest;
import com.aliyun.oss.model.UploadFileResult;
import com.genersoft.iot.vmp.conf.OssConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import jakarta.annotation.PreDestroy;
import java.io.File;

@Slf4j
@Component
public class OssUtil {

    private static final String DEFAULT_ENDPOINT = "oss-cn-hangzhou.aliyuncs.com";
    private static final long MIN_PART_SIZE = 5L * 1024 * 1024;
    private static final long DEFAULT_PART_SIZE = 10L * 1024 * 1024;
    private static final int DEFAULT_TASK_NUM = 3;

    @Autowired
    private OssConfig ossConfig;

    private volatile OSS ossClient;

    @PreDestroy
    public void destroy() {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }

    public boolean isConfigured() {
        return StringUtils.hasText(ossConfig.getAccessKeyId())
                && StringUtils.hasText(ossConfig.getAccessKeySecret())
                && StringUtils.hasText(ossConfig.getBucketName());
    }

    private OSS getOssClient() {
        if (ossClient == null) {
            synchronized (this) {
                if (ossClient == null) {
                    ossClient = new OSSClientBuilder().build(
                            getEndpoint(),
                            ossConfig.getAccessKeyId(),
                            ossConfig.getAccessKeySecret(),
                            buildClientConfiguration());
                }
            }
        }
        return ossClient;
    }

    public String buildRecordObjectName(String app, String stream, String fileName) {
        return String.format("record/%s/%s/%s", app, stream, fileName);
    }

    public String uploadFile(File file, String objectName) {
        if (!isConfigured()) {
            throw new IllegalStateException("OSS参数未配置完整");
        }
        if (file == null || !file.exists() || file.length() <= 0) {
            throw new IllegalArgumentException("待上传文件不存在或为空");
        }

        try {
            PutObjectRequest putObjectRequest = new PutObjectRequest(ossConfig.getBucketName(), objectName, file);
            PutObjectResult result = getOssClient().putObject(putObjectRequest);
            log.info("[OSS上传] 简单上传成功，ETag: {}", result.getETag());
            return buildObjectUrl(objectName);
        } catch (OSSException oe) {
            log.error("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason.");
            log.error("Error Message: {}", oe.getErrorMessage());
            log.error("Error Code: {}", oe.getErrorCode());
            log.error("Request ID: {}", oe.getRequestId());
            log.error("Host ID: {}", oe.getHostId());
            throw new RuntimeException("上传文件到OSS失败", oe);
        } catch (ClientException ce) {
            log.error("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
            log.error("Error Message: {}", ce.getMessage());
            throw new RuntimeException("上传文件到OSS失败", ce);
        } catch (Throwable e) {
            throw new RuntimeException("上传文件到OSS失败", e);
        }
    }

    public String uploadFileResumable(File file, String objectName) {
        if (!isConfigured()) {
            throw new IllegalStateException("OSS参数未配置完整");
        }
        if (file == null || !file.exists() || file.length() <= 0) {
            throw new IllegalArgumentException("待上传文件不存在或为空");
        }

        File checkpointFile = buildCheckpointFile(file, objectName);
        try {
            UploadFileRequest uploadFileRequest = new UploadFileRequest(ossConfig.getBucketName(), objectName);
            uploadFileRequest.setUploadFile(file.getAbsolutePath());
            uploadFileRequest.setTaskNum(DEFAULT_TASK_NUM);
            uploadFileRequest.setPartSize(calculatePartSize(file.length()));
            uploadFileRequest.setEnableCheckpoint(true);
            uploadFileRequest.setCheckpointFile(checkpointFile.getAbsolutePath());

            UploadFileResult result = getOssClient().uploadFile(uploadFileRequest);
            String eTag = result != null && result.getMultipartUploadResult() != null
                    ? result.getMultipartUploadResult().getETag() : "N/A";
            log.info("[OSS上传] 分片上传成功，ETag: {}, checkpoint: {}", eTag, checkpointFile.getAbsolutePath());
            deleteQuietly(checkpointFile);
            return buildObjectUrl(objectName);
        } catch (OSSException oe) {
            log.error("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason.");
            log.error("Error Message: {}", oe.getErrorMessage());
            log.error("Error Code: {}", oe.getErrorCode());
            log.error("Request ID: {}", oe.getRequestId());
            log.error("Host ID: {}", oe.getHostId());
            throw new RuntimeException("分片上传文件到OSS失败", oe);
        } catch (ClientException ce) {
            log.error("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
            log.error("Error Message: {}", ce.getMessage());
            throw new RuntimeException("分片上传文件到OSS失败", ce);
        } catch (Throwable e) {
            throw new RuntimeException("分片上传文件到OSS失败", e);
        }
    }

    public String buildObjectUrl(String objectName) {
        return "https://" + ossConfig.getBucketName() + "." + getCleanEndpoint() + "/" + objectName;
    }

    private ClientBuilderConfiguration buildClientConfiguration() {
        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        conf.setSocketTimeout(60 * 1000); // 缩短Socket超时至1分钟
        conf.setConnectionTimeout(10 * 1000); // 缩短连接超时至10秒
        conf.setMaxErrorRetry(2); // 缩短重试次数至2次，避免因网络不通导致卡住过久
        conf.setConnectionRequestTimeout(10 * 1000); // 缩短从连接池获取连接的超时至10秒
        return conf;
    }

    private String getEndpoint() {
        return StringUtils.hasText(ossConfig.getEndpoint()) ? ossConfig.getEndpoint() : DEFAULT_ENDPOINT;
    }

    private String getCleanEndpoint() {
        return getEndpoint().replaceFirst("^https?://", "");
    }

    private long calculatePartSize(long fileSize) {
        if (fileSize <= 100L * 1024 * 1024) {
            return DEFAULT_PART_SIZE;
        }
        long dynamicPartSize = fileSize / 10000;
        return Math.max(MIN_PART_SIZE, dynamicPartSize);
    }

    private File buildCheckpointFile(File sourceFile, String objectName) {
        String tempDir = System.getProperty("java.io.tmpdir");
        String safeName = objectName.replace("/", "_");
        String fileName = "oss-upload-" + safeName + "-" + sourceFile.length() + ".cp";
        return new File(tempDir, fileName);
    }

    private void deleteQuietly(File file) {
        if (file != null && file.exists() && !file.delete()) {
            file.deleteOnExit();
        }
    }
}
