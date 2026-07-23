# Vehicle OSS Callback Spec

## Why

目前白天时段（2:00-22:00）的录像仅上传切片并触发视频分析回调。现需针对车载设备（设备编号后三位大于等于110）在凌晨4点至中午12点之间的录像，增加回调通知，但传递的 analysis 字段需强制为 false，不触发视频分析回调。

## What Changes

* 修改 `UploadFileInfo` 类，增加 `analysis` 属性。

* 修改 `buildUploadFile` 方法，增加 `stream` 参数，解析设备编号后三位，判断是否为车载设备（>=110）。

* 对于车载设备且文件生成完成时间在 4\~12 点之间的情况，设置 `notifyVideoReceive=true`，且 `analysis=false`。

* 修改 `notifyVideoReceive` 方法，接收 `analysis` 参数并传递给外部接口。

## Impact

* Affected specs: 视频上传与回调逻辑

* Affected code: `src/main/java/com/genersoft/iot/vmp/media/zlm/listener/OssUploadEventListener.java`

## ADDED Requirements

### Requirement: 新增车载设备视频回调

The system SHALL provide video callback for vehicle devices (stream ends with >= 110) between 4 AM and 12 PM, with analysis set to false.

#### Scenario: Success case

* **WHEN** file generation completes between 4 AM and 12 PM, and stream ends with >= 110

* **THEN** trigger `notifyVideoReceive` with `analysis=false`

## MODIFIED Requirements

### Requirement: 现有视频回调逻辑

修改回调方法的入参，从全局配置中读取改为通过 `UploadFileInfo` 传递 `analysis` 标志。
