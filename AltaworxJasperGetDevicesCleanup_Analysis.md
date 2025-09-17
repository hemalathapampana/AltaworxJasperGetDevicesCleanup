# AltaworxJasperGetDevicesCleanup Lambda Analysis

## Overview
The `AltaworxJasperGetDevicesCleanup` Lambda function is responsible for processing device synchronization cleanup tasks for various carrier integrations including Jasper, ThingSpace, Telegence, eBonding, Teal, and Pond.

---

## 1. What triggers cleanup Lambda (SQS publisher details)?

### SQS Trigger Configuration
- **Queue URL Environment Variable**: `DeviceNotificationQueueURL`
- **Backup Queue URL Environment Variable**: `CarrierOptimizationQueueURL`

### SQS Message Attributes Expected
The Lambda expects SQS messages with the following attributes:

```csharp
// Required Attributes
- IntegrationType: Integration type ID (Jasper, ThingSpace, etc.)
- ServiceProviderId: Service provider identifier
- RetryCount: Current retry attempt count
- MaxRetries: Maximum retry attempts allowed (default: 14)
- DelayBetweenRetries: Delay in seconds between retries (default: 300)

// Optional Attributes
- RemainingRowsToProcess: Number of remaining rows to process
- ShouldQueueCarrierOptimization: Boolean flag for carrier optimization
- OptimizationSessionId: Session ID for optimization process
```

### Message Processing Flow
1. **SQS Event Handler**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
2. **Single Message Processing**: Expects exactly one message per event
3. **Message Parsing**: `GetDevicesCleanupSqsValues(context, message)` extracts SQS attributes
4. **Retry Logic**: Implements exponential backoff with configurable max retries

---

## 2. Sequence of stored procedures and dependencies

### Integration-Specific Stored Procedures

#### Jasper Integration (IntegrationType: Jasper, POD19, TMobileJasper, Rogers)
```sql
1. usp_Jasper_DeviceSync
   - Parameters: @JasperDbName, @ServiceProviderId
   - Timeout: SQLConstant.TimeoutSeconds
   - Uses Transaction: Yes
```

#### ThingSpace Integration
```sql
1. usp_ThingSpace_DeviceSync
   - Parameters: @ServiceProviderId
   - Timeout: 900 seconds
```

#### Telegence Integration (Sequential execution required)
```sql
1. usp_Telegence_Update_Device (@ServiceProviderId) - Timeout: 1800s
2. usp_Telegence_Update_DeviceDetail (@ServiceProviderId) - Timeout: 1800s
3. usp_Update_TelegenceDeviceMobilityFeature_FromStaging - Timeout: 800s
4. usp_Telegence_Update_DeviceUsage_FromStaging (@ServiceProviderId) - Timeout: 1800s
5. usp_Telegence_Update_DeviceMubuUsage_FromStaging (@ServiceProviderId) - Timeout: 1800s
6. usp_Telegence_DeviceSync (@ServiceProviderId) - Timeout: 1800s
```

#### eBonding Integration
```sql
1. usp_eBonding_Update_DeviceUsage_FromStaging (@ServiceProviderId) - Timeout: 60s
2. usp_eBonding_DeviceSync (@ServiceProviderId) - Timeout: 90s
```

#### Teal Integration
```sql
1. usp_Teal_Device_Sync (@ServiceProviderId) - Timeout: SQLConstant.TimeoutSeconds
```

#### Pond Integration
```sql
1. POND_DEVICE_SYNC (@ServiceProviderId) - Timeout: SQLConstant.TimeoutSeconds
```

### Common Post-Processing Procedures

#### For Mobility Integrations (eBonding, Telegence)
```sql
usp_MobilityDeviceSync_Common (@IntegrationId) - Timeout: 900s
```

#### For M2M Integrations (All others)
```sql
usp_DeviceSync_Common (@IntegrationId) - Timeout: 900s
```

### Summary Generation Procedures
```sql
- usp_ThingSpace_Devices_Get_Sync_Summary
- usp_Telegence_Devices_Get_Sync_Summary
- usp_eBonding_Devices_Get_Sync_Summary
- usp_Teal_Devices_Get_Sync_Summary
- usp_Jasper_Devices_Get_Sync_Summary (@ServiceProviderId)
- usp_Pond_Devices_Get_Sync_Summary
```

---

## 3. How are new communication plans detected — exact query logic?

### Detection Query
New communication plans are detected using this SQL query:

```sql
SELECT DISTINCT d.CommunicationPlan
FROM [dbo].[Device] d
LEFT JOIN [dbo].[JasperCommunicationPlan] jcp ON jcp.CommunicationPlanName = d.CommunicationPlan
WHERE d.ServiceProviderId = @ServiceProviderId
  AND jcp.CommunicationPlanName IS NULL
  AND d.CommunicationPlan IS NOT NULL
```

### Detection Logic
1. **Left Join**: Devices table with JasperCommunicationPlan table
2. **Filter Criteria**:
   - Service Provider matches current provider
   - Communication plan exists in Device table
   - Communication plan does NOT exist in JasperCommunicationPlan table (IS NULL)

### Auto-Creation Process
When new communication plans are detected:

```sql
INSERT INTO [dbo].[JasperCommunicationPlan] 
(CommunicationPlanName, CreatedBy, CreatedDate, IsActive, IsDeleted, ServiceProviderId)
VALUES (@CommPlan, 'JasperDeviceSync', GETUTCDATE(), 1, 0, @ServiceProviderId)
```

### Notification
- **Email Subject**: "New Communication Plans Added"
- **Email Content**: Lists all newly detected communication plans
- **Purpose**: Alerts administrators to associate new plans with rate plans for optimization

---

## 4. Where are sync summary reports stored, and what details are included?

### Storage Locations

#### Email Summary Reports
- **Recipients**: Configured via `DeviceSyncToEmailAddresses` (semicolon-separated)
- **Sender**: `DeviceSyncFromEmailAddress`
- **Subject**: `DeviceSyncResultsEmailSubject` (dynamically replaced with integration name)

#### S3 Storage (Device Sync Summary Logs)
- **Bucket**: Configured via `DEVICE_SYNC_SUMMARY_LOG_S3_BUCKET_NAME`
- **Enable Flag**: `DEVICE_SYNC_SUMMARY_LOG_ENABLE`
- **File Naming**: `{IntegrationType}-{yyyyMMdd}.txt`
  - Examples: `Jasper-20241201.txt`, `ThingSpace-20241201.txt`

### Summary Details Included

#### Email Summary Content
```csharp
DeviceSyncSummary {
    DateTime? DetailLastSyncDate     // Last device detail sync timestamp
    int? DetailQueueCount           // Number of device details in queue
    int? DetailUpdatedCount         // Number of device details updated
    DateTime? UsageLastSyncDate     // Last usage sync timestamp
    int? UsageQueueCount           // Number of usage records in queue
    int? UsageUpdatedCount         // Number of usage records updated
    int? DeviceCount               // Total device count
}
```

#### S3 Log File Content (CSV Format)
```
ID,ICCID,MSISDN,RawDataUsage,CurrentDataUsage,RawSMSUsage,CurrentSMSUsage,RawVoiceUsage,CurrentVoiceUsage,BillingCycleStartDate,BillingCycleEndDate,UsageDate,ServiceProviderId
```

**Fields Description**:
- **ID**: Device identifier
- **ICCID**: Integrated Circuit Card Identifier
- **MSISDN**: Mobile Station International Subscriber Directory Number
- **RawDataUsage**: Raw data usage from carrier
- **CurrentDataUsage**: Processed data usage in system
- **RawSMSUsage**: Raw SMS usage from carrier
- **CurrentSMSUsage**: Processed SMS usage in system
- **RawVoiceUsage**: Raw voice usage from carrier
- **CurrentVoiceUsage**: Processed voice usage in system
- **BillingCycleStartDate**: Billing period start date
- **BillingCycleEndDate**: Billing period end date
- **UsageDate**: Usage record date
- **ServiceProviderId**: Service provider identifier

### Snowflake Historian Data
For non-mobility integrations with Snowflake configuration:
- **Bucket**: `SnowflakeS3BucketName`
- **Path**: `SnowflakeS3BucketPath`
- **File Format**: CSV with device history data
- **File Naming**: `DeviceHistory_{ServiceProviderId}_{yyyyMMddhhmmss}.csv`

---

## 5. What API call/endpoint sends the final sync completion notification to AMOP 2.0?

### API Endpoint Configuration
- **Environment Variable**: `AMOP_20_SYNC_UPDATE_API_URL_KEY`
- **Service Class**: `DailySyncAmopApiTrigger`
- **Method**: `SendNotificationToAmop20()`

### API Call Details

#### HTTP Request Configuration
```csharp
// HTTP Client Setup
HttpClient client = new HttpClient(new LambdaLoggingHandler())
client.BaseAddress = new Uri(amop20SyncUpdateApiUrl)
client.DefaultRequestHeaders.Add("Accept", "application/json")

// HTTP Method
POST to client.BaseAddress
```

#### Request Payload
```json
{
  "data": {
    "key_name": "{keyName}",
    "tenant_id": "{tenantId}",
    "tenant_name": "{tenantName}"
  }
}
```

#### Key Name Mapping
Based on integration type:
```csharp
switch (IntegrationId) {
    case IntegrationType.ThingSpace:
        keyName = "thingspace_devices";
    case IntegrationType.Telegence:
        keyName = "telegence_devices";
    case IntegrationType.Teal:
        keyName = "teal_devices";
    case IntegrationType.Jasper:
    case IntegrationType.POD19:
    case IntegrationType.TMobileJasper:
    case IntegrationType.Rogers:
        keyName = "jasper_devices";
    case IntegrationType.Pond:
        keyName = "pond_inventories";
}
```

#### Response Handling
- **Success**: Logs "Sent Response to AMOP2.0"
- **Error**: Logs response body for debugging
- **Content-Type**: `application/json`
- **Encoding**: UTF-8

### Trigger Timing
The AMOP 2.0 notification is sent **after all processing completes**, regardless of success or failure, ensuring AMOP 2.0 is always notified of sync completion.

---

## Process Flow Summary

1. **SQS Message Reception** → Parse message attributes
2. **Row Count Check** → Verify remaining processing items
3. **Retry Logic** → Handle incomplete processing
4. **Device Sync** → Execute integration-specific stored procedures
5. **Communication Plans** → Detect and create new plans
6. **Summary Generation** → Create sync summary data
7. **Email Notification** → Send summary email
8. **S3 Logging** → Upload detailed logs (if enabled)
9. **Snowflake Export** → Send device history (if configured)
10. **Carrier Optimization** → Queue optimization tasks (if applicable)
11. **AMOP 2.0 Notification** → Send completion notification
12. **Cleanup** → Resource cleanup and logging

This comprehensive analysis covers all aspects of the AltaworxJasperGetDevicesCleanup Lambda function's operation and dependencies.