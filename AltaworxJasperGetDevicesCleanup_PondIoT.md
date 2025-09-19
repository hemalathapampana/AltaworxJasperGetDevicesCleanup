## AltaworxJasperGetDevicesCleanup — Pond IoT Device Cleanup Flow (AltaworxJasperGetDevicesCleanup)

This document explains the responsibilities, inputs/outputs, control flow, side effects, and error handling for the key functions and helpers involved in the Pond IoT device cleanup flow implemented by the Lambda “AltaworxJasperGetDevicesCleanup”. It is tailored to Pond IoT integration specifics, following the system design.

### Contents
- Entry/Initialization
  - Function.InitializeServices
- Event Processing
  - Function.ProcessEventAsync
  - Function.ProcessEventRecordAsync
  - Function.GetMessageQueueValues
  - GetDevicesCleanupSqsValues (data contract)
- Sync and Common Ops
  - Function.SyncDeviceTables
  - Function.SyncPondIoTDevices
  - Function.CountRowsToProcess
  - Function.IsTooManyRetries
  - GeneratePondIoTDeviceSyncSummary
- Email / Reporting
  - Function.SendEmailAsync
  - Function.GetSummaryValues
  - Function.SendEmailSummaryAsync
- GeneralProviderSettings (config)
- IntegrationTypeRepository.GetIntegrationTypes
- External Integrations
  - DailySyncAmopApiTrigger.SendNotificationToAmop20

### Function.InitializeServices

- **Purpose**: Initialize all services and configuration needed for processing Pond IoT cleanup messages.

- **Responsibilities**:
  - Read environment variables used by Pond IoT processing and summary logging.
  - Initialize logging toggles for device sync summary logs.
  - Initialize S3 client/wrapper for summary CSV uploads.
  - Initialize settings/config repositories (e.g., email recipients, integration type metadata).
  - Prepare Base64 or utility services used downstream for attachments/log payloads.

- **Key Environment**:
  - `DEVICE_SYNC_SUMMARY_LOG_S3_BUCKET_NAME` (string) — target S3 bucket for CSV logs
  - `DEVICE_SYNC_SUMMARY_LOG_ENABLE` (bool) — enable/disable summary log generation
  - `DeviceNotificationQueueURL` (string) — SQS URL used for requeue on retry
  - `ConnectionString`, `BaseMultiTenantConnectionString`, `PORTAL_CONNECTION_STRING` — DB connections
  - `SnowflakeS3BucketName`, `SnowflakeS3BucketPath` — for historian export (used elsewhere)
  - `AMOP_20_SYNC_UPDATE_API_URL_KEY` — AMOP 2.0 endpoint

- **Inputs/Outputs**:
  - Input: `KeySysLambdaContext` (returned by BaseFunctionHandler), `IConfiguration`/environment
  - Output: None (sets up services on the Function instance/context)

- **Side Effects**:
  - Creates clients for S3/SQS/SES as required.
  - Reads and caches configuration values on the Function instance/context.

- **Error Handling**:
  - Missing required environment variables are logged as errors; execution may abort early.
  - Non-fatal optional settings (e.g., log enable) default to safe values.

### Function.ProcessEventAsync

- **Purpose**: Entry point for handling a batch of SQS messages (AWS Lambda SQS trigger). Iterates messages and delegates to per-record processing.

- **Responsibilities**:
  - Iterate `SQSEvent.Records`.
  - For each record, call `ProcessEventRecordAsync` with SQL retry policy active.
  - Aggregate success/failure metrics; ensure failures are logged with sufficient context.

- **Inputs/Outputs**:
  - Input: `KeySysLambdaContext`, `SQSEvent`
  - Output: Task (no return value) — completion indicates the batch is processed

- **Side Effects**:
  - Logs per-batch and per-message progress.
  - May requeue individual items based on retry policy within `ProcessEventRecordAsync` logic.

- **Error Handling**:
  - Catches unhandled exceptions to avoid Lambda batch failure where possible.
  - Uses SQL retry policy (Polly) for transient DB issues.

### Function.ProcessEventRecordAsync

- **Purpose**: Process a single SQS message for Pond IoT cleanup: parse, validate, decide retry/continue, perform sync, summarize, notify.

- **Responsibilities**:
  - Parse SQS attributes/body using `GetMessageQueueValues` into `GetDevicesCleanupSqsValues`.
  - Resolve integration type (must be Pond IoT) using `IntegrationTypeRepository.GetIntegrationTypes`.
  - Count rows remaining; check retry ceilings/backoff via `CountRowsToProcess` and `IsTooManyRetries`.
  - Execute main Pond IoT sync via `SyncDeviceTables` (invokes `SyncPondIoTDevices` + common sync SP).
  - Generate Pond IoT CSV summary to S3 if logging enabled.
  - Update communication plans and send “new plans” email if applicable.
  - Gather sync summary with `GetSummaryValues` and send email via `SendEmailSummaryAsync`.
  - Trigger AMOP 2.0 notification for downstream systems.

- **Inputs/Outputs**:
  - Input: `KeySysLambdaContext`, SQS message
  - Output: Task (no return value)

- **Side Effects**:
  - Database writes via stored procedures.
  - S3 CSV file uploads for sync summary.
  - SES emails to admins.
  - Optional SQS requeue when retries are warranted.

- **Error Handling**:
  - Pond IoT–specific SQL timeout logic (`SqlException.Number == -2`) → log + conditional requeue.
  - Transient SQL exceptions handled by retry policy.
  - Non-critical failures (email/S3) logged and processing continues.

### Function.GetMessageQueueValues

- **Purpose**: Extract and validate Pond IoT–specific attributes from the SQS message into a strongly-typed contract.

- **Expected Attributes**:
  - `IntegrationType`: must be “PondIoT” (exact value per system configuration)
  - `ServiceProviderId`: numeric/string identifier of the Pond IoT service provider
  - `RetryCount`, `MaxRetries`, `DelayBetweenRetries`: retry controls
  - `RemainingRowsToProcess`: integer hint; Pond IoT path may bypass per-ICCIDs batching

- **Inputs/Outputs**:
  - Input: `KeySysLambdaContext`, SQS message
  - Output: `GetDevicesCleanupSqsValues`

- **Validation**:
  - Throws/logs error if `IntegrationType != PondIoT`.
  - Ensures numeric attributes parse; defaults applied for missing optional attributes.

### GetDevicesCleanupSqsValues (data contract)

- **Fields**:
  - `IntegrationType`: string — must be “PondIoT”
  - `ServiceProviderId`: int/string — DB scoping for Pond IoT SP
  - `RetryCount`: int — current attempt
  - `MaxRetries`: int — ceiling for auto-retry
  - `DelayBetweenRetries`: int (seconds) — backoff per attempt
  - `RemainingRowsToProcess`: int — informational
  - Optional: `TenantId`, `TenantName` — used for AMOP notification if provided

- **Semantics**:
  - Captures all routing and retry controls from the SQS layer; no per-ICCIDs batching for Pond IoT in this Lambda.

### Function.SyncDeviceTables

- **Purpose**: Orchestrate database synchronization for Pond IoT by running Pond IoT–specific stored procedure followed by common M2M normalization.

- **Responsibilities**:
  - Execute `SyncPondIoTDevices` (Pond IoT stored procedure: `usp_PondIoT_Device_Sync`).
  - Execute `usp_DeviceSync_Common` for cross-carrier normalization.
  - Apply standard SQL timeout values: Device sync (900s), Common (300s).

- **Inputs/Outputs**:
  - Input: `KeySysLambdaContext`, `GetDevicesCleanupSqsValues`, sqlRetryPolicy
  - Output: Task

- **Error Handling**:
  - Catches `SqlException` timeout (`-2`) to support targeted requeue.
  - Retries transient errors via policy.

### Function.SyncPondIoTDevices

- **Purpose**: Execute the Pond IoT device sync stored procedure `usp_PondIoT_Device_Sync` for a given `ServiceProviderId`.

- **Stored Procedure**:
  - Name: `usp_PondIoT_Device_Sync`
  - Timeout: 900 seconds (STANDARD_TIMEOUT)
  - Parameters: `@ServiceProviderId`

- **Operations Performed by SP (summary)**:
  - Stage processing from Pond IoT/Jasper staging tables
  - Merge/Update `Device` table
  - Sync usage information
  - Update statuses/connectivity
  - Validate data integrity
  - Cleanup processed staging records
  - Handle billing period attach/creation with incomplete data rules

### Function.CountRowsToProcess

- **Purpose**: Determine count of remaining rows/items relevant to Pond IoT processing; used for retry governance and visibility logging.

- **Notes**:
  - Pond IoT path typically runs set-based sync (no per-ICCIDs batching here). RemainingRows may reflect staging/backlog estimation.

- **Outputs**:
  - Integer count of outstanding rows (as known to the system).

### Function.IsTooManyRetries

- **Purpose**: Decide whether to stop processing and requeue based on current retry count and max configured attempts; may incorporate remaining rows and backoff.

- **Logic**:
  - If `RetryCount >= MaxRetries` → stop retries for this message.
  - Otherwise, apply exponential backoff using `DelayBetweenRetries` and possibly `RemainingRowsToProcess`.

### GeneratePondIoTDeviceSyncSummary

- **Purpose**: Generate Pond IoT–specific CSV summary of device usage deltas after sync and upload to S3.

- **Output**:
  - S3 object: `PondIoTDeviceSync_{ServiceProviderId}_{yyyyMMdd_HHmmss}.csv`
  - Columns: `DeviceId, ICCID, MSISDN, CurrentUsage, PreviousUsage, UsageDelta, LastSyncTimestamp, BillingPeriod`

- **Source Query (representative)**

```sql
SELECT d.Id,
       d.ICCID,
       d.MSISDN,
       d.CurrentDataUsage AS CurrentUsage,
       d.PreviousDataUsage AS PreviousUsage,
       (d.CurrentDataUsage - d.PreviousDataUsage) AS UsageDelta,
       d.LastSyncDate      AS LastSyncTimestamp,
       d.BillingCycleDate  AS BillingPeriod
FROM Device d
WHERE d.ServiceProviderId = @ServiceProviderId
  AND d.LastSyncDate >= @SyncStartTime
ORDER BY d.LastSyncDate DESC;
```

- **Error Handling**:
  - S3 upload failures are logged and retried per internal retry (if configured); failure does not fail the entire sync.

### Function.SendEmailAsync

- **Purpose**: Send email notification about newly discovered Pond IoT communication plans that require attention (rate plan mapping, etc.).

- **Trigger**:
  - After sync, when new values exist in `Device.CommunicationPlan` not present in `JasperCommunicationPlan`.

- **Content**:
  - Subject: “New Communication Plans Added - Pond IoT”
  - Body: List of new plan names and basic counts
  - Recipients: From `GeneralProviderSettings`/`ServiceProviderSetting`

- **Representative Detection Query**

```sql
SELECT DISTINCT d.CommunicationPlan
FROM [dbo].[Device] d
LEFT JOIN [dbo].[JasperCommunicationPlan] jcp
  ON jcp.CommunicationPlanName = d.CommunicationPlan
WHERE d.ServiceProviderId = @ServiceProviderId
  AND jcp.CommunicationPlanName IS NULL
  AND d.CommunicationPlan IS NOT NULL;
```

- **Error Handling**:
  - Email send errors are logged; sync continues.

### Function.GetSummaryValues

- **Purpose**: Retrieve summary metrics for Pond IoT sync to be used in the email summary report.

- **Implementation**:
  - Executes `usp_PondIoT_Devices_Get_Sync_Summary @ServiceProviderId`.

### Function.SendEmailSummaryAsync

- **Purpose**: Send HTML/text email summarizing Pond IoT device sync results.

- **Inputs**:
  - `KeySysLambdaContext`
  - IntegrationType (Pond IoT) and full integration type list (for subject customization)
  - Pond IoT SyncSummary values
  - `shouldGoToHistorian` (bool): include historian status details

- **Content**:
  - Subject: “Pond IoT Device Sync Summary - [ServiceProviderName]”
  - HTML Table:
    - Service Provider, Sync Date
    - Total Devices
    - Details Updated, Usage Records Updated
    - Last Detail Sync, Last Usage Sync
    - Optional: Snowflake export status

- **Error Handling**:
  - Log on failure; do not fail overall processing.

### GeneralProviderSettings (config)

- **Purpose**: Provide per-tenant/service provider settings including email recipients, sender addresses, and notification preferences.

- **Relevant Settings**:
  - `DeviceSyncFromEmail`
  - `DeviceSyncToEmail`
  - `DeviceSyncEmailSubject` (override)

- **Provisioning Example**

```sql
INSERT INTO ServiceProviderSetting (ServiceProviderId, SettingName, SettingValue) VALUES
(@PondIoTServiceProviderId, 'DeviceSyncFromEmail', 'noreply@company.com'),
(@PondIoTServiceProviderId, 'DeviceSyncToEmail',   'admin@company.com'),
(@PondIoTServiceProviderId, 'DeviceSyncEmailSubject', 'Pond IoT Device Sync Summary');
```

### IntegrationTypeRepository.GetIntegrationTypes

- **Purpose**: Load integration types and status flags from DB to validate that Pond IoT integration exists and is active.

- **Usage**:
  - Subject customization and conditional logic based on integration attributes.

- **Output**:
  - Collection of `IntegrationType { Id, Name, IsActive, … }`

- **Error Handling**:
  - Log and abort Pond IoT path if Pond IoT type not found/active.

### DailySyncAmopApiTrigger.SendNotificationToAmop20

- **Purpose**: Inform AMOP 2.0 of Pond IoT sync completion for downstream ingestion/historian processes.

- **Behavior**:
  - HTTP POST to configured AMOP 2.0 endpoint with JSON payload:

```json
{
  "key_name": "pond_iot_devices",
  "tenant_id": "[TenantId]",
  "tenant_name": "[TenantName]",
  "sync_timestamp": "[ISO8601DateTime]",
  "service_provider_id": "[ServiceProviderId]"
}
```

- **Notes**:
  - No Polly HTTP retry configured in this project; failures are logged.

### Execution Notes (Pond IoT specifics)

- No per-ICCIDs batching in Lambda; set-based `usp_PondIoT_Device_Sync` + `usp_DeviceSync_Common`.
- Retry policy: SQL transient retry up to 3 attempts; timeouts logged and may trigger requeue.
- Summary CSV is optional and gated by `DEVICE_SYNC_SUMMARY_LOG_ENABLE` and S3 bucket config.
- Snowflake export path is handled outside this function list; mention included for completeness.

### Glossary of Stored Procedures

- `usp_PondIoT_Device_Sync` — Pond IoT detail/usage merge, billing period attach, archive stale devices, write daily usage.
- `usp_DeviceSync_Common` — Normalize device/status history across carriers.
- `usp_PondIoT_Devices_Get_Sync_Summary` — Return last sync dates, queue counts, updated counts, device count.

### Errors and Retries (Pond IoT)

- SQL timeout (`-2`): logged as “Pond IoT sync timeout”; if under `MaxRetries`, message is requeued with backoff.
- Data validation errors: logged; processing continues to next items.
- S3/email failures: logged; do not fail the sync.

### Sample Logging Messages

- “[PondIoT] Processing SQS message for ServiceProviderId={id}, Retry={r}/{max}”
- “[PondIoT] Executing usp_PondIoT_Device_Sync (timeout=900s)”
- “[PondIoT] usp_DeviceSync_Common completed”
- “[PondIoT] Summary CSV uploaded to s3://{bucket}/{key}”
- “[PondIoT] Email summary sent to {recipients}”
- “[PondIoT] AMOP 2.0 notified for tenant={tenantId}”

### Security and Compliance

- Ensure least-privilege IAM policies for S3 (write to summary bucket) and SES send-email.
- Protect DB connection strings via AWS Secrets Manager/SSM where possible.
- Validate and sanitize message attributes; treat all external inputs as untrusted.