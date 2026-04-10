package com.genersoft.iot.vmp.service;

import java.io.InputStream;

public interface IOssService {
    String uploadFile(String objectName, InputStream stream);
}
