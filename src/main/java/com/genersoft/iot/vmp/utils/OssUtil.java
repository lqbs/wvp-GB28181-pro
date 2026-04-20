package com.genersoft.iot.vmp.utils;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.UploadFileRequest;
import com.genersoft.iot.vmp.conf.OssConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.File;

@Component
public class OssUtil {

    private static final String DEFAULT_ENDPOINT = "oss-cn-hangzhou.aliyuncs.com";
    private static final long DEFAULT_PART_SIZE = 10 * 1024 * 1024L;
    private static final int DEFAULT_TASK_NUM = 5;

    @Autowired
    private OssConfig ossConfig;

    public boolean isConfigured() {
        return StringUtils.hasText(ossConfig.getAccessKeyId())
                && StringUtils.hasText(ossConfig.getAccessKeySecret())
                && StringUtils.hasText(ossConfig.getBucketName());
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

        String endpoint = getEndpoint();
        File checkpointFile = new File(file.getAbsolutePath() + ".ucp");
        OSS ossClient = null;
        try {
            ossClient = new OSSClientBuilder().build(
                    endpoint,
                    ossConfig.getAccessKeyId(),
                    ossConfig.getAccessKeySecret(),
                    buildClientConfiguration()
            );

            UploadFileRequest uploadFileRequest = new UploadFileRequest(ossConfig.getBucketName(), objectName);
            uploadFileRequest.setUploadFile(file.getAbsolutePath());
            uploadFileRequest.setPartSize(DEFAULT_PART_SIZE);
            uploadFileRequest.setTaskNum(DEFAULT_TASK_NUM);
            uploadFileRequest.setEnableCheckpoint(true);
            uploadFileRequest.setCheckpointFile(checkpointFile.getAbsolutePath());

            ossClient.uploadFile(uploadFileRequest);
            return buildObjectUrl(objectName);
        } catch (Throwable e) {
            throw new RuntimeException("上传文件到OSS失败", e);
        } finally {
            if (ossClient != null) {
                ossClient.shutdown();
            }
            deleteQuietly(checkpointFile);
        }
    }

    public String buildObjectUrl(String objectName) {
        return "https://" + ossConfig.getBucketName() + "." + getCleanEndpoint() + "/" + objectName;
    }

    private ClientBuilderConfiguration buildClientConfiguration() {
        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        conf.setSocketTimeout(5 * 60 * 1000);
        conf.setConnectionTimeout(60 * 1000);
        conf.setMaxErrorRetry(5);
        conf.setConnectionRequestTimeout(60 * 1000);
        return conf;
    }

    private String getEndpoint() {
        return StringUtils.hasText(ossConfig.getEndpoint()) ? ossConfig.getEndpoint() : DEFAULT_ENDPOINT;
    }

    private String getCleanEndpoint() {
        return getEndpoint().replaceFirst("^https?://", "");
    }

    private void deleteQuietly(File file) {
        if (file != null && file.exists() && !file.delete()) {
            file.deleteOnExit();
        }
    }
}
