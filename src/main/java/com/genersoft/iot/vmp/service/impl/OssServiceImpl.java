package com.genersoft.iot.vmp.service.impl;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.PutObjectRequest;
import com.genersoft.iot.vmp.conf.OssConfig;
import com.genersoft.iot.vmp.service.IOssService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.InputStream;

@Slf4j
@Service
public class OssServiceImpl implements IOssService {

    @Autowired
    private OssConfig ossConfig;

    @Override
    public String uploadFile(String objectName, InputStream stream) {
        if (!ossConfig.isEnable()) {
            return null;
        }

        if (objectName != null && objectName.startsWith("/")) {
            objectName = objectName.substring(1);
        }

        OSS ossClient = null;
        try {
            ossClient = new OSSClientBuilder().build(
                    ossConfig.getEndpoint(),
                    ossConfig.getAccessKeyId(),
                    ossConfig.getAccessKeySecret()
            );

            PutObjectRequest putObjectRequest = new PutObjectRequest(ossConfig.getBucketName(), objectName, stream);
            ossClient.putObject(putObjectRequest);

            String endpoint = ossConfig.getEndpoint();
            String bucketName = ossConfig.getBucketName();
            String url;
            
            if (endpoint.startsWith("http://")) {
                url = "http://" + bucketName + "." + endpoint.substring(7) + "/" + objectName;
            } else if (endpoint.startsWith("https://")) {
                url = "https://" + bucketName + "." + endpoint.substring(8) + "/" + objectName;
            } else {
                url = "http://" + bucketName + "." + endpoint + "/" + objectName;
            }
            
            return url;

        } catch (Exception e) {
            log.error("文件上传 OSS 失败: {}", objectName, e);
            return null;
        } finally {
            if (ossClient != null) {
                ossClient.shutdown();
            }
        }
    }
}
