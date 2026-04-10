package com.genersoft.iot.vmp.media.zlm.listener;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.PutObjectRequest;
import com.genersoft.iot.vmp.conf.OssConfig;
import com.genersoft.iot.vmp.media.bean.MediaServer;
import com.genersoft.iot.vmp.media.bean.RecordInfo;
import com.genersoft.iot.vmp.media.event.media.MediaRecordMp4Event;
import com.genersoft.iot.vmp.media.service.IMediaServerService;
import com.genersoft.iot.vmp.service.bean.DownloadFileInfo;
import com.genersoft.iot.vmp.storager.dao.CloudRecordServiceMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.InputStream;

@Slf4j
@Component
public class OssUploadEventListener {

    @Autowired
    private OssConfig ossConfig;

    @Autowired
    private IMediaServerService mediaServerService;

    @Autowired
    private CloudRecordServiceMapper cloudRecordServiceMapper;

    private final OkHttpClient client = new OkHttpClient();

    @Async("taskExecutor")
    @EventListener
    public void onApplicationEvent(MediaRecordMp4Event event) {
        if (!StringUtils.hasText(ossConfig.getAccessKeyId()) || !StringUtils.hasText(ossConfig.getBucketName())) {
            log.debug("[OSS上传] 未配置OSS参数，忽略上传");
            return;
        }

        RecordInfo recordInfo = event.getRecordInfo();
        MediaServer mediaServer = event.getMediaServer();
        String app = event.getApp();
        String stream = event.getStream();
        String fileName = recordInfo.getFileName();

        log.info("[OSS上传] 开始处理上传任务: {}/{}/{}", app, stream, fileName);

        // 等待几秒钟，确保CloudRecordServiceImpl已经插入数据库记录
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            log.error("[OSS上传] 睡眠中断", e);
        }

        // 更新状态为上传中
        cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 1, null);

        DownloadFileInfo downloadFileInfo = mediaServerService.getDownloadFilePath(mediaServer, recordInfo);
        String downloadUrl = downloadFileInfo.getHttpPath();
        if (!StringUtils.hasText(downloadUrl)) {
            log.error("[OSS上传] 无法获取下载地址: {}/{}/{}", app, stream, fileName);
            cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 3, null);
            return;
        }

        OSS ossClient = null;
        try {
            // 通过HTTP GET获取视频流
            Request request = new Request.Builder()
                    .url(downloadUrl)
                    .build();

            Response response = client.newCall(request).execute();
            if (!response.isSuccessful()) {
                log.error("[OSS上传] 获取视频文件失败, HTTP状态码: {}", response.code());
                cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 3, null);
                return;
            }

            ResponseBody body = response.body();
            if (body == null) {
                log.error("[OSS上传] 获取视频文件失败, 响应体为空");
                cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 3, null);
                return;
            }

            InputStream inputStream = body.byteStream();

            // 上传到OSS
            String endpoint = ossConfig.getEndpoint();
            if (!StringUtils.hasText(endpoint)) {
                // 如果没有配置endpoint，可以使用默认的或者根据需要处理
                endpoint = "oss-cn-hangzhou.aliyuncs.com";
            }
            
            // 去除 endpoint 中的 http:// 或 https:// 前缀
            String cleanEndpoint = endpoint.replaceFirst("^https?://", "");
            
            ossClient = new OSSClientBuilder().build(endpoint, ossConfig.getAccessKeyId(), ossConfig.getAccessKeySecret());
            
            // OSS中的对象路径，例如 record/app/stream/2026-04-10-13-16-51-1.mp4
            String objectName = "record/" + app + "/" + stream + "/" + fileName;

            PutObjectRequest putObjectRequest = new PutObjectRequest(ossConfig.getBucketName(), objectName, inputStream);
            ossClient.putObject(putObjectRequest);

            // 构造OSS访问URL
            String ossUrl = "https://" + ossConfig.getBucketName() + "." + cleanEndpoint + "/" + objectName;
            log.info("[OSS上传] 成功上传至OSS: {}", ossUrl);

            // 更新状态为上传成功
            cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 2, ossUrl);

        } catch (Exception e) {
            log.error("[OSS上传] 上传至OSS失败", e);
            // 更新状态为上传失败
            cloudRecordServiceMapper.updateOssInfo(app, stream, fileName, 3, null);
        } finally {
            if (ossClient != null) {
                ossClient.shutdown();
            }
        }
    }
}
