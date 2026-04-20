package com.genersoft.iot.vmp.conf;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "video-receive", ignoreInvalidFields = true)
public class VideoReceiveConfig {

    private boolean enabled = true;

    private String url = "https://server-test.hoomi.cn/warehouse/sortingTask/weighingStation/video/receive";

    private boolean analysis = false;
}
