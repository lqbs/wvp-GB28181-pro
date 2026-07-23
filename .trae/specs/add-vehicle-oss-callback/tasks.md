# Tasks
- [x] Task 1: Modify `UploadFileInfo` and `notifyVideoReceive` signatures.
  - [x] SubTask 1.1: Add `analysis` boolean to `UploadFileInfo`.
  - [x] SubTask 1.2: Add `analysis` boolean to `notifyVideoReceive` and use it in JSON body.
- [x] Task 2: Update `buildUploadFile` to implement vehicle logic.
  - [x] SubTask 2.1: Pass `stream` to `buildUploadFile`.
  - [x] SubTask 2.2: Extract last 3 digits of `stream` to determine if it's a vehicle (>= 110).
  - [x] SubTask 2.3: Return `notifyVideoReceive=true` and `analysis=false` for vehicles between 4 and 12.
- [x] Task 3: Update `onApplicationEvent` to use the new parameters.
  - [x] SubTask 3.1: Pass `stream` to `buildUploadFile`.
  - [x] SubTask 3.2: Pass `uploadFileInfo.isAnalysis()` to `notifyVideoReceive`.

# Task Dependencies
- Task 2 depends on Task 1
- Task 3 depends on Task 1 and 2