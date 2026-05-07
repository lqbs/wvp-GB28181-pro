package com.genersoft.iot.vmp.media.zlm.listener;

import com.alibaba.fastjson2.JSONObject;
import com.genersoft.iot.vmp.conf.VideoReceiveConfig;
import com.genersoft.iot.vmp.media.bean.MediaServer;
import com.genersoft.iot.vmp.media.bean.RecordInfo;
import com.genersoft.iot.vmp.media.service.IMediaServerService;
import com.genersoft.iot.vmp.media.event.media.MediaRecordMp4Event;
import com.genersoft.iot.vmp.storager.dao.CloudRecordServiceMapper;
import com.genersoft.iot.vmp.utils.OssUtil;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
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

    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
    private static final long MIN_UPLOAD_FILE_SIZE = 200L * 1024 * 1024;

    @Autowired
    private OssUtil ossUtil;

    @Autowired
    private CloudRecordServiceMapper cloudRecordServiceMapper;

    @Autowired
    private VideoReceiveConfig videoReceiveConfig;

    @Autowired
    private IMediaServerService mediaServerService;

    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
            .readTimeout(300, java.util.concurrent.TimeUnit.SECONDS)
            .build();

    @Async("taskExecutor")
    @EventListener
    public void onApplicationEvent(MediaRecordMp4Event event) {
        if (!ossUtil.isConfigured()) {
            log.debug("[OSS上传] 未配置OSS参数，忽略上传");
            return;
        }

        RecordInfo recordInfo = event.getRecordInfo();
        String app = event.getApp();
        String stream = event.getStream();
        String fileName = recordInfo.getFileName();
        String filePath = recordInfo.getFilePath();

        if (recordInfo.getFileSize() > 0 && recordInfo.getFileSize() < MIN_UPLOAD_FILE_SIZE) {
            log.info("[OSS上传] 文件小于200MB，跳过上传: {}/{}/{}, 大小: {} MB",
                    app, stream, fileName, recordInfo.getFileSize() / 1024 / 1024);
            return;
        }

        log.info("[OSS上传] 开始处理上传任务: {}/{}/{}", app, stream, fileName);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[OSS上传] 睡眠中断", e);
        }

        MediaServer mediaServer = event.getMediaServer();
        if (mediaServer == null) {
            log.error("[OSS上传] 缺少MediaServer信息，无法构建下载URL");
            cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 3, null);
            return;
        }

        String downloadUrl = String.format("http://%s:%s/index/api/downloadFile?file_path=%s", mediaServer.getIp(),
                mediaServer.getHttpPort(), filePath);
        if (StringUtils.hasText(mediaServer.getSecret())) {
            downloadUrl += "&secret=" + mediaServer.getSecret();
        }
        log.info("[OSS上传] 视频URL: {}", downloadUrl);

        File uploadFile = null;
        boolean tempFileCreated = false;
        try {
            File localFile = new File(filePath);
            if (localFile.exists() && localFile.length() > 0) {
                if (localFile.length() < MIN_UPLOAD_FILE_SIZE) {
                    log.info("[OSS上传] 本地文件小于200MB，跳过上传: {}, 大小: {} MB",
                            localFile.getAbsolutePath(), localFile.length() / 1024 / 1024);
                    return;
                }
                log.info("[OSS上传] 检测到本地文件，直接上传: {}, 大小: {} MB",
                        localFile.getAbsolutePath(), localFile.length() / 1024 / 1024);
                uploadFile = localFile;
            } else {
                uploadFile = downloadToTempFile(downloadUrl);
                tempFileCreated = true;
                if (uploadFile.length() < MIN_UPLOAD_FILE_SIZE) {
                    log.info("[OSS上传] 下载文件小于200MB，跳过上传: {}, 大小: {} MB",
                            uploadFile.getAbsolutePath(), uploadFile.length() / 1024 / 1024);
                    return;
                }
            }

            cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 1, null);
            String objectName = ossUtil.buildRecordObjectName(app, stream, fileName);
            log.info("[OSS上传] 开始分片上传到OSS(支持断点续传): {}", objectName);
            long start = System.currentTimeMillis();
            String ossUrl = ossUtil.uploadFileResumable(uploadFile, objectName);
            log.info("[OSS上传] 上传完成, 耗时: {} 秒", (System.currentTimeMillis() - start) / 1000);
            log.info("[OSS上传] 成功上传至OSS: {}", ossUrl);
            cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 2, ossUrl);
            boolean notifySuccess = notifyVideoReceive(stream, ossUrl);
            if (notifySuccess) {
                deleteRecordSourceFile(event, recordInfo);
            }

        } catch (Throwable e) {
            log.error("[OSS上传] 上传至OSS失败", e);
            cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 3, null);
        } finally {
            if (tempFileCreated) {
                deleteQuietly(uploadFile);
            }
        }
    }

    private void deleteRecordSourceFile(MediaRecordMp4Event event, RecordInfo recordInfo) {
        if (event.getMediaServer() == null || !StringUtils.hasText(recordInfo.getFilePath())) {
            log.warn("[OSS上传] 删除录制视频源文件失败，缺少媒体节点或文件路径信息: {}", recordInfo.getFilePath());
            return;
        }

        File sourceFile = new File(recordInfo.getFilePath());
        File parentFile = sourceFile.getParentFile();
        if (parentFile == null) {
            log.warn("[OSS上传] 删除录制视频源文件失败，无法解析录像日期目录: {}", recordInfo.getFilePath());
            return;
        }

        boolean deleteResult = mediaServerService.deleteRecordDirectory(
                event.getMediaServer(),
                event.getApp(),
                event.getStream(),
                parentFile.getName(),
                recordInfo.getFileName());
        if (deleteResult) {
            log.info("[OSS上传] 删除录制视频源文件成功: {}", recordInfo.getFilePath());
        } else {
            log.warn("[OSS上传] 删除录制视频源文件失败: {}", recordInfo.getFilePath());
        }
    }

    private boolean notifyVideoReceive(String stream, String ossUrl) {
        if (!videoReceiveConfig.isEnabled() || !StringUtils.hasText(videoReceiveConfig.getUrl())) {
            log.debug("[视频回调] 未启用或未配置回调地址，忽略发送");
            return false;
        }

        String deviceCode = stream;

        JSONObject body = new JSONObject();
        body.put("deviceCode", deviceCode);
        body.put("videoUrl", ossUrl);
        body.put("analysis", videoReceiveConfig.isAnalysis());

        Request request = new Request.Builder()
                .url(videoReceiveConfig.getUrl())
                .post(RequestBody.create(body.toJSONString(), JSON_MEDIA_TYPE))
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String responseBody = response.body() != null ? response.body().string() : "";
                log.error("[视频回调] 调用失败, code: {}, body: {}", response.code(), responseBody);
                return false;
            }
            String responseBody = response.body() != null ? response.body().string() : "";
            log.info("[视频回调] 调用成功, deviceCode: {}, response: {}", deviceCode, responseBody);
            return true;
        } catch (Exception e) {
            log.error("[视频回调] 调用异常, deviceCode: {}, videoUrl: {}", deviceCode, ossUrl, e);
            return false;
        }
    }

    private File downloadToTempFile(String downloadUrl) throws Exception {
        File tempFile = File.createTempFile("oss-upload-", ".mp4");
        log.info("[OSS上传] 开始下载到临时文件: {}", tempFile.getAbsolutePath());

        try {
            Request request = new Request.Builder().url(downloadUrl).build();
            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful() || response.body() == null) {
                    throw new IllegalStateException("获取视频文件失败, HTTP状态码: " + response.code());
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
            return tempFile;
        } catch (Exception e) {
            deleteQuietly(tempFile);
            throw e;
        }
    }

    private void deleteQuietly(File f) {
        if (f != null && f.exists() && !f.delete()) {
            f.deleteOnExit();
        }
    }
}
