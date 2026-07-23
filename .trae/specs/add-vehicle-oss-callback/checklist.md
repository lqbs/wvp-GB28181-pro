* [x] `UploadFileInfo` 包含 `analysis` 字段及 getter

* [x] `notifyVideoReceive` 方法接收并使用 `analysis` 参数

* [x] `buildUploadFile` 能够正确解析 `stream` 后三位

* [x] 车载设备（后三位 >= 110）在 4\~12 点之间返回 `notifyVideoReceive=true` 和 `analysis=false`

* [x] 非车载设备或非 4\~12 点保持原有逻辑

* [x] `onApplicationEvent` 正确传递参数

