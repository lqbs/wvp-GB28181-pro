package com.genersoft.iot.vmp.media.zlm.listener;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.UploadFileRequest;
import com.genersoft.iot.vmp.conf.OssConfig;
import com.genersoft.iot.vmp.media.bean.RecordInfo;
import com.genersoft.iot.vmp.media.event.media.MediaRecordMp4Event;
import com.genersoft.iot.vmp.storager.dao.CloudRecordServiceMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

@Slf4j
@Component
public class OssUploadEventListener {

    @Autowired
    private OssConfig ossConfig;

    @Autowired
    private CloudRecordServiceMapper cloudRecordServiceMapper;

    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
            .readTimeout(300, java.util.concurrent.TimeUnit.SECONDS)
            .build();

    @Async("taskExecutor")
    @EventListener
    public void onApplicationEvent(MediaRecordMp4Event event) {
        if (!StringUtils.hasText(ossConfig.getAccessKeyId()) || !StringUtils.hasText(ossConfig.getBucketName())) {
            log.debug("[OSS上传] 未配置OSS参数，忽略上传");
            return;
        }

        RecordInfo recordInfo = event.getRecordInfo();
        String app = event.getApp();
        String stream = event.getStream();
        String fileName = recordInfo.getFileName();
        String filePath = recordInfo.getFilePath();

        log.info("[OSS上传] 开始处理上传任务: {}/{}/{}", app, stream, fileName);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[OSS上传] 睡眠中断", e);
        }

        cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 1, null);

        String ip = "121.43.133.173";
        String downloadUrl = String.format("http://%s:8080/mediaserver/api/downloadFile?file_path=%s", ip, filePath);
        log.info("[OSS上传] 视频URL: {}", downloadUrl);

        File tempFile = null;
        File checkpointFile = null;
        OSS ossClient = null;

        try {
            String endpoint = StringUtils.hasText(ossConfig.getEndpoint())
                    ? ossConfig.getEndpoint()
                    : "oss-cn-hangzhou.aliyuncs.com";
            String cleanEndpoint = endpoint.replaceFirst("^https?://", "");

            ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
            conf.setSocketTimeout(5 * 60 * 1000);
            conf.setConnectionTimeout(60 * 1000);
            conf.setMaxErrorRetry(5);
            conf.setConnectionRequestTimeout(60 * 1000);

            ossClient = new OSSClientBuilder().build(
                    endpoint, ossConfig.getAccessKeyId(), ossConfig.getAccessKeySecret(), conf);

            File localFile = new File(filePath);
            if (localFile.exists() && localFile.length() > 0) {
                log.info("[OSS上传] 检测到本地文件，直接上传: {}, 大小: {} MB",
                        localFile.getAbsolutePath(), localFile.length() / 1024 / 1024);
                tempFile = localFile;
            } else {
                tempFile = File.createTempFile("oss-upload-", ".mp4");
                log.info("[OSS上传] 开始下载到临时文件: {}", tempFile.getAbsolutePath());

                Request request = new Request.Builder().url(downloadUrl).build();
                try (Response response = client.newCall(request).execute()) {
                    if (!response.isSuccessful() || response.body() == null) {
                        log.error("[OSS上传] 获取视频文件失败, HTTP状态码: {}", response.code());
                        cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 3, null);
                        return;
                    }
                    try (InputStream in = response.body().byteStream();
                            FileOutputStream out = new FileOutputStream(tempFile)) {
                        byte[] buf = new byte[64 * 1024];
                        int len;
                        long total = 0;
                        while ((len = in.read(buf)) != -1) {
                            out.write(buf, 0, len);
                            total += len;
                        }
                        log.info("[OSS上传] 下载完成, 大小: {} MB", total / 1024 / 1024);
                    }
                }
            }

            String objectName = "record/" + app + "/" + stream + "/" + fileName;
            checkpointFile = new File(tempFile.getAbsolutePath() + ".ucp");

            UploadFileRequest uploadFileRequest = new UploadFileRequest(
                    ossConfig.getBucketName(), objectName);
            uploadFileRequest.setUploadFile(tempFile.getAbsolutePath());
            uploadFileRequest.setPartSize(10 * 1024 * 1024L);
            uploadFileRequest.setTaskNum(5);
            uploadFileRequest.setEnableCheckpoint(true);
            uploadFileRequest.setCheckpointFile(checkpointFile.getAbsolutePath());

            log.info("[OSS上传] 开始分片上传到OSS: {}", objectName);
            long start = System.currentTimeMillis();
            ossClient.uploadFile(uploadFileRequest);
            log.info("[OSS上传] 分片上传完成, 耗时: {} 秒", (System.currentTimeMillis() - start) / 1000);

            String ossUrl = "https://" + ossConfig.getBucketName() + "." + cleanEndpoint + "/" + objectName;
            log.info("[OSS上传] 成功上传至OSS: {}", ossUrl);
            cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 2, ossUrl);

        } catch (Throwable e) {
            log.error("[OSS上传] 上传至OSS失败", e);
            cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 3, null);
        } finally {
            if (ossClient != null) {
                ossClient.shutdown();
            }
            deleteQuietly(tempFile);
            deleteQuietly(checkpointFile);
        }
    }

    private void deleteQuietly(File f) {
        if (f != null && f.exists() && !f.delete()) {
            f.deleteOnExit();
        }
    }
}
