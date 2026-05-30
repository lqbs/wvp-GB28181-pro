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
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * 录像 MP4 文件生成后的 OSS 上传监听器。
 *
 * <p>处理规则：
 * <ul>
 *     <li>收到 {@link MediaRecordMp4Event} 后立即进入 OSS 上传处理。</li>
 *     <li>按文件生成完成时间判断时段，完成时间 = 开始时间 + 文件时长。</li>
 *     <li>02:00 <= hour < 22:00：先用 ffmpeg 切出文件前 1800 秒，只上传切片，不调用视频分析接口。</li>
 *     <li>22:00 <= hour 或 hour < 02:00：上传完整文件；完整文件小于 100MB 时跳过上传。</li>
 *     <li>只有夜间完整文件成功上传 OSS 后，才调用视频分析接口。</li>
 * </ul>
 */
@Slf4j
@Component
public class OssUploadEventListener {

    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");

    /**
     * 白天时段上传到 OSS 的截取时长，单位：秒。
     */
    private static final long DAYTIME_SLICE_SECONDS = 1800;

    /**
     * 夜间完整文件上传的最小文件大小。只有夜间完整文件保留此限制，白天切片不受此限制。
     */
    private static final long MIN_UPLOAD_FILE_SIZE = 100L * 1024 * 1024;

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

    /**
     * 处理录像文件生成事件。
     *
     * <p>优先使用本地文件路径上传；如果当前服务无法直接读取本地文件，则通过流媒体节点下载接口
     * 拉取到临时文件后再处理。临时下载文件和白天切片文件会在 finally 中清理。
     */
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

        log.info("[OSS上传] 开始处理上传任务: {}/{}/{}", app, stream, fileName);

        MediaServer mediaServer = event.getMediaServer();
        if (mediaServer == null) {
            log.error("[OSS上传] 缺少MediaServer信息，无法构建下载URL");
            updateOssInfo(app, stream, fileName, 3, null);
            return;
        }

        String downloadUrl = String.format("http://%s:%s/index/api/downloadFile?file_path=%s", mediaServer.getIp(),
                mediaServer.getHttpPort(), filePath);
        if (StringUtils.hasText(mediaServer.getSecret())) {
            downloadUrl += "&secret=" + mediaServer.getSecret();
        }
        log.info("[OSS上传] 视频URL: {}", downloadUrl);

        File uploadFile = null;
        File sourceFile = null;
        File downloadedTempFile = null;
        try {
            File localFile = new File(filePath);
            if (localFile.exists() && localFile.length() > 0) {
                log.info("[OSS上传] 检测到本地文件: {}, 大小: {} MB",
                        localFile.getAbsolutePath(), localFile.length() / 1024 / 1024);
                sourceFile = localFile;
            } else {
                // 文件不在本机时，从媒体节点下载到临时目录后再执行时段分流和上传。
                downloadedTempFile = downloadToTempFile(downloadUrl);
                sourceFile = downloadedTempFile;
            }

            UploadFileInfo uploadFileInfo = buildUploadFile(recordInfo, sourceFile);
            if (uploadFileInfo == null) {
                return;
            }
            uploadFile = uploadFileInfo.getFile();
            updateOssInfo(app, stream, fileName, 1, null);
            String objectName = ossUtil.buildRecordObjectName(app, stream, fileName);
            log.info("[OSS上传] 开始分片上传到OSS(支持断点续传): {}", objectName);
            long start = System.currentTimeMillis();
            String ossUrl = ossUtil.uploadFileResumable(uploadFile, objectName);
            log.info("[OSS上传] 上传完成, 耗时: {} 秒", (System.currentTimeMillis() - start) / 1000);
            log.info("[OSS上传] 成功上传至OSS: {}", ossUrl);
            updateOssInfo(app, stream, fileName, 2, ossUrl);
            // 只有夜间完整文件上传完成后才进入视频分析回调，白天切片上传不触发分析。
            boolean notifySuccess = uploadFileInfo.isNotifyVideoReceive() && notifyVideoReceive(stream, ossUrl);
            if (notifySuccess) {
                deleteRecordSourceFile(event, recordInfo);
            }

        } catch (Throwable e) {
            log.error("[OSS上传] 上传至OSS失败", e);
            updateOssInfo(app, stream, fileName, 3, null);
        } finally {
            if (uploadFile != null && uploadFile != sourceFile) {
                deleteQuietly(uploadFile);
            }
            if (downloadedTempFile != null) {
                deleteQuietly(downloadedTempFile);
            }
        }
    }

    /**
     * 更新录像记录中的 OSS 上传状态。
     *
     * <p>录像入库监听器和 OSS 上传监听器都是异步事件监听，上传侧可能先于入库侧执行。
     * 这里做短重试，避免刚开始更新状态时数据库记录尚未插入导致状态丢失。
     */
    private void updateOssInfo(String app, String stream, String fileName, Integer uploadStatus, String ossUrl) {
        for (int i = 0; i < 10; i++) {
            int rows = cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, uploadStatus, ossUrl);
            if (rows > 0) {
                return;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[OSS上传] 更新OSS状态等待中断: {}/{}/{}", app, stream, fileName);
                return;
            }
        }
        log.warn("[OSS上传] 更新OSS状态未命中录像记录: {}/{}/{}, status: {}", app, stream, fileName, uploadStatus);
    }

    /**
     * 按文件生成完成时间选择实际上传文件。
     *
     * @return 返回待上传文件及后续回调策略；返回 null 表示按业务规则跳过上传
     */
    private UploadFileInfo buildUploadFile(RecordInfo recordInfo, File sourceFile) throws Exception {
        int completeHour = getRecordCompleteHour(recordInfo);
        // 白天生成完成的文件只上传前30分钟切片，避免完整文件进入OSS和分析流程。
        if (completeHour >= 2 && completeHour < 22) {
            File slicedFile = sliceFirstSeconds(sourceFile, recordInfo.getFileName(), DAYTIME_SLICE_SECONDS);
            log.info("[OSS上传] 文件生成完成时间小时为{}，上传前{}秒切片: {} -> {}",
                    completeHour, DAYTIME_SLICE_SECONDS, sourceFile.getAbsolutePath(), slicedFile.getAbsolutePath());
            return new UploadFileInfo(slicedFile, false);
        }
        log.info("[OSS上传] 文件生成完成时间小时为{}，夜间时段上传完整文件: {}", completeHour, sourceFile.getAbsolutePath());
        // 夜间完整文件保留100MB门槛，达到门槛后上传并触发视频分析回调。
        if (sourceFile.length() < MIN_UPLOAD_FILE_SIZE) {
            log.info("[OSS上传] 夜间完整文件小于100MB，跳过上传: {}, 大小: {} MB",
                    sourceFile.getAbsolutePath(), sourceFile.length() / 1024 / 1024);
            return null;
        }
        return new UploadFileInfo(sourceFile, true);
    }

    /**
     * 待上传文件和上传成功后的附加动作。
     *
     * <p>notifyVideoReceive=true 表示该文件上传成功后需要调用视频分析接口。
     * 目前只有夜间完整文件满足这个条件。
     */
    private static class UploadFileInfo {
        private final File file;
        private final boolean notifyVideoReceive;

        private UploadFileInfo(File file, boolean notifyVideoReceive) {
            this.file = file;
            this.notifyVideoReceive = notifyVideoReceive;
        }

        public File getFile() {
            return file;
        }

        public boolean isNotifyVideoReceive() {
            return notifyVideoReceive;
        }
    }

    /**
     * 获取录像文件生成完成时间所在小时。
     *
     * <p>RecordInfo 中没有独立的完成时间字段，因此使用 startTime + timeLen 计算。
     */
    private int getRecordCompleteHour(RecordInfo recordInfo) {
        long completeTime = recordInfo.getStartTime() + (long) recordInfo.getTimeLen();
        return Instant.ofEpochMilli(completeTime).atZone(ZoneId.systemDefault()).getHour();
    }

    /**
     * 使用 ffmpeg 从源文件头部截取指定秒数。
     *
     * <p>使用 -c copy 直接拷贝音视频流，避免转码带来的额外 CPU 消耗；截取失败或超时时抛出异常，
     * 由外层统一标记 OSS 上传失败。
     */
    private File sliceFirstSeconds(File sourceFile, String fileName, long seconds) throws Exception {
        File slicedFile = File.createTempFile("oss-upload-slice-", getFileSuffix(fileName));
        ProcessBuilder processBuilder = new ProcessBuilder(
                "ffmpeg",
                "-y",
                "-i", sourceFile.getAbsolutePath(),
                "-t", String.valueOf(seconds),
                "-c", "copy",
                "-movflags", "+faststart",
                slicedFile.getAbsolutePath());
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        Process process = processBuilder.start();
        boolean finished = process.waitFor(Math.max(60, seconds / 10), TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            deleteQuietly(slicedFile);
            throw new IllegalStateException("ffmpeg切片超时: " + sourceFile.getAbsolutePath());
        }
        if (process.exitValue() != 0 || !slicedFile.exists() || slicedFile.length() <= 0) {
            deleteQuietly(slicedFile);
            throw new IllegalStateException("ffmpeg切片失败: " + sourceFile.getAbsolutePath() + ", exitCode=" + process.exitValue());
        }
        return slicedFile;
    }

    /**
     * 生成临时切片文件后缀，尽量沿用原始文件后缀。
     */
    private String getFileSuffix(String fileName) {
        if (StringUtils.hasText(fileName)) {
            int index = fileName.lastIndexOf('.');
            if (index >= 0 && index < fileName.length() - 1) {
                return fileName.substring(index);
            }
        }
        return ".mp4";
    }

    /**
     * 视频分析回调成功后，删除媒体节点上的源录像文件。
     *
     * <p>白天切片上传不会触发视频分析回调，因此也不会走到这里删除源文件。
     */
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

    /**
     * 调用外部视频接收/分析接口。
     *
     * <p>调用方会控制触发条件：只有夜间完整文件成功上传 OSS 后才调用此方法。
     */
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

    /**
     * 从媒体节点下载录像文件到本机临时目录。
     *
     * <p>当 recordInfo.filePath 指向的文件不在本机或无法直接读取时使用。
     */
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

    /**
     * 静默删除临时文件，删除失败时注册 JVM 退出时删除。
     */
    private void deleteQuietly(File f) {
        if (f != null && f.exists() && !f.delete()) {
            f.deleteOnExit();
        }
    }
}
