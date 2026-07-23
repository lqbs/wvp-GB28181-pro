package com.genersoft.iot.vmp.media.zlm.listener;

import com.alibaba.fastjson2.JSONObject;
import com.genersoft.iot.vmp.conf.OssConfig;
import com.genersoft.iot.vmp.conf.VideoReceiveConfig;
import com.genersoft.iot.vmp.storager.dao.CloudRecordServiceMapper;
import com.genersoft.iot.vmp.media.service.IMediaServerService;
import com.genersoft.iot.vmp.utils.OssUtil;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = OssUploadEventListenerTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class OssUploadEventListenerTest {

    private static final String STREAM = "34020000002000000105";
    private static final String VIDEO_URL =
            "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/"
                    + "34020000002000000003_34020000001320000003/2026-04-22-06-42-27-23.mp4";
    private static final List<VideoReceiveItem> VIDEO_RECEIVE_ITEMS = List.of(
            new VideoReceiveItem("34020000002000000109_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000109_34020000001320000001/2026-07-19-23-44-08-6.mp4"),
            new VideoReceiveItem("34020000002000000109_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000109_34020000001320000001/2026-07-20-00-44-12-7.mp4"),
            new VideoReceiveItem("34020000002000000108_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000108_34020000001320000001/2026-07-19-23-44-06-6.mp4"),
            new VideoReceiveItem("34020000002000000108_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000108_34020000001320000001/2026-07-20-00-44-10-7.mp4"),
            new VideoReceiveItem("34020000002000000107_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000107_34020000001320000001/2026-07-19-23-44-08-6.mp4"),
            new VideoReceiveItem("34020000002000000107_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000107_34020000001320000001/2026-07-20-00-44-11-7.mp4"),
            new VideoReceiveItem("34020000002000000106_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000106_34020000001320000001/2026-07-19-23-44-08-6.mp4"),
            new VideoReceiveItem("34020000002000000106_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000106_34020000001320000001/2026-07-20-00-44-11-7.mp4"),
            new VideoReceiveItem("34020000002000000105_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000105_34020000001320000001/2026-07-19-23-43-50-6.mp4"),
            new VideoReceiveItem("34020000002000000105_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000105_34020000001320000001/2026-07-20-00-43-51-7.mp4"),
            new VideoReceiveItem("34020000002000000104_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000104_34020000001320000001/2026-07-19-23-44-08-6.mp4"),
            new VideoReceiveItem("34020000002000000104_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000104_34020000001320000001/2026-07-20-00-44-12-7.mp4"),
            new VideoReceiveItem("34020000002000000103_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000103_34020000001320000001/2026-07-19-23-44-08-6.mp4"),
            new VideoReceiveItem("34020000002000000103_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000103_34020000001320000001/2026-07-20-00-44-12-7.mp4"),
            new VideoReceiveItem("34020000002000000102_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000102_34020000001320000001/2026-07-19-23-44-07-6.mp4"),
            new VideoReceiveItem("34020000002000000102_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000102_34020000001320000001/2026-07-20-00-44-11-7.mp4"),
            new VideoReceiveItem("34020000002000000101_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000101_34020000001320000001/2026-07-19-23-44-07-6.mp4"),
            new VideoReceiveItem("34020000002000000101_34020000001320000001", "https://hoomi-media-server.oss-cn-hangzhou.aliyuncs.com/record/rtp/34020000002000000101_34020000001320000001/2026-07-20-00-44-11-7.mp4")
    );

    private static final HttpServer CALLBACK_SERVER = createCallbackServer();
    private static final AtomicReference<String> REQUEST_METHOD = new AtomicReference<>();
    private static final AtomicReference<String> REQUEST_PATH = new AtomicReference<>();
    private static final AtomicReference<String> REQUEST_CONTENT_TYPE = new AtomicReference<>();
    private static final List<String> REQUEST_BODIES = new CopyOnWriteArrayList<>();
    private static CountDownLatch requestReceived = new CountDownLatch(VIDEO_RECEIVE_ITEMS.size());

    @Autowired
    private OssUploadEventListener listener;

    @Autowired
    private VideoReceiveConfig videoReceiveConfig;

    @BeforeEach
    void setUp() {
        REQUEST_METHOD.set(null);
        REQUEST_PATH.set(null);
        REQUEST_CONTENT_TYPE.set(null);
        REQUEST_BODIES.clear();
        requestReceived = new CountDownLatch(VIDEO_RECEIVE_ITEMS.size());
    }

    @AfterAll
    static void tearDown() {
        CALLBACK_SERVER.stop(0);
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("video-receive.enabled", () -> "true");
        registry.add("video-receive.analysis", () -> "true");
        registry.add("video-receive.url", OssUploadEventListenerTest::getCallbackUrl);
    }

    @Test
    void notifyVideoReceive_shouldPostEveryStreamAndVideoUrlInBatch() throws Exception {
        for (VideoReceiveItem item : VIDEO_RECEIVE_ITEMS) {
            Boolean result = ReflectionTestUtils.invokeMethod(
                    listener, "notifyVideoReceive", item.stream(), item.videoUrl());
            assertThat(result).isTrue();
        }

        assertThat(videoReceiveConfig.isAnalysis()).isTrue();
        assertThat(requestReceived.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(REQUEST_METHOD.get()).isEqualTo("POST");
        assertThat(REQUEST_PATH.get()).isEqualTo("/video/receive");
        assertThat(REQUEST_CONTENT_TYPE.get()).startsWith("application/json");
        assertThat(REQUEST_BODIES).hasSize(VIDEO_RECEIVE_ITEMS.size());

        List<JSONObject> bodies = REQUEST_BODIES.stream().map(JSONObject::parseObject).toList();
        for (VideoReceiveItem item : VIDEO_RECEIVE_ITEMS) {
            assertThat(bodies).anySatisfy(body -> {
                assertThat(body.getString("deviceCode")).isEqualTo(item.stream());
                assertThat(body.getString("videoUrl")).isEqualTo(item.videoUrl());
                assertThat(body.getBooleanValue("analysis")).isTrue();
            });
        }
    }

    private static String getCallbackUrl() {
        return "https://server-prod.hoomi.cn/warehouse/sortingTask/weighingStation/video/receive";
    }

    private static HttpServer createCallbackServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/video/receive", OssUploadEventListenerTest::handleCallback);
            server.start();
            return server;
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void handleCallback(HttpExchange exchange) throws IOException {
        try {
            REQUEST_METHOD.set(exchange.getRequestMethod());
            REQUEST_PATH.set(exchange.getRequestURI().getPath());
            REQUEST_CONTENT_TYPE.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            REQUEST_BODIES.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));

            byte[] responseBody = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBody.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(responseBody);
            }
        } finally {
            exchange.close();
            requestReceived.countDown();
        }
    }

    private record VideoReceiveItem(String stream, String videoUrl) {
    }

    @SpringBootConfiguration
    @Import(TestConfig.class)
    static class TestApplication {
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties({VideoReceiveConfig.class, OssConfig.class})
    static class TestConfig {

        @Bean
        OssUploadEventListener ossUploadEventListener() {
            return new OssUploadEventListener();
        }

        @Bean
        OssUtil ossUtil() {
            return new OssUtil();
        }

        @Bean
        CloudRecordServiceMapper cloudRecordServiceMapper() {
            return noOpProxy(CloudRecordServiceMapper.class);
        }

        @Bean
        IMediaServerService mediaServerService() {
            return noOpProxy(IMediaServerService.class);
        }

        private static <T> T noOpProxy(Class<T> type) {
            return type.cast(Proxy.newProxyInstance(
                    type.getClassLoader(),
                    new Class<?>[]{type},
                    (proxy, method, args) -> defaultValue(method.getReturnType())));
        }

        private static Object defaultValue(Class<?> type) {
            if (!type.isPrimitive()) {
                return null;
            }
            if (type == boolean.class) {
                return false;
            }
            if (type == char.class) {
                return '\0';
            }
            if (type == byte.class) {
                return (byte) 0;
            }
            if (type == short.class) {
                return (short) 0;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            if (type == float.class) {
                return 0F;
            }
            if (type == double.class) {
                return 0D;
            }
            return null;
        }
    }
}
