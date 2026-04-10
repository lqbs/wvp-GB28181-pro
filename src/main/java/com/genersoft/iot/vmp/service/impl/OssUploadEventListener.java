package com.genersoft.iot.vmp.service.impl;

import com.genersoft.iot.vmp.media.event.media.MediaRecordMp4Event;
import com.genersoft.iot.vmp.service.IOssService;
import com.genersoft.iot.vmp.service.bean.CloudRecordItem;
import com.genersoft.iot.vmp.storager.dao.CloudRecordServiceMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

@Slf4j
@Component
public class OssUploadEventListener {

    @Autowired
    private IOssService ossService;

    @Autowired
    private CloudRecordServiceMapper cloudRecordServiceMapper;

    @Async("taskExecutor")
    @EventListener
    public void onApplicationEvent(MediaRecordMp4Event event) {
        String app = event.getApp();
        String stream = event.getStream();
        String fileName = event.getRecordInfo().getFileName();
        String filePath = event.getRecordInfo().getFilePath();

        log.info("[OSS上传] 接收到录像完成事件，准备上传: {}/{} - {}", app, stream, fileName);

        CloudRecordItem recordItem = null;
        for (int i = 0; i < 5; i++) {
            recordItem = cloudRecordServiceMapper.getListByFileName(app, stream, fileName);
            if (recordItem != null) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("[OSS上传] 休眠被中断", e);
                Thread.currentThread().interrupt();
            }
        }

        if (recordItem == null) {
            log.warn("[OSS上传] 未找到对应的云端录像记录: {}/{} - {}", app, stream, fileName);
            return;
        }

        File file = new File(filePath);
        if (!file.exists()) {
            log.warn("[OSS上传] 文件不存在: {}", filePath);
            cloudRecordServiceMapper.updateUploadStatusAndOssUrl(recordItem.getId(), 3, null); 
            return;
        }

        try (InputStream inputStream = new FileInputStream(file)) {
            cloudRecordServiceMapper.updateUploadStatusAndOssUrl(recordItem.getId(), 1, null);
            
            String objectName = app + "/" + stream + "/" + fileName;
            
            String url = ossService.uploadFile(objectName, inputStream);
            
            if (url != null) {
                log.info("[OSS上传] 上传成功，访问地址: {}", url);
                cloudRecordServiceMapper.updateUploadStatusAndOssUrl(recordItem.getId(), 2, url);
            } else {
                log.warn("[OSS上传] 上传失败");
                cloudRecordServiceMapper.updateUploadStatusAndOssUrl(recordItem.getId(), 3, null);
            }
        } catch (Exception e) {
            log.error("[OSS上传] 上传过程发生异常", e);
            if (recordItem != null) {
                cloudRecordServiceMapper.updateUploadStatusAndOssUrl(recordItem.getId(), 3, null);
            }
        }
    }
}
