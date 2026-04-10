package com.genersoft.iot.vmp.conf;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "oss", ignoreInvalidFields = true)
public class OssConfig {
    private String accessKeyId;
    private String accessKeySecret;
    private String endpoint;
    private String bucketName;
}
