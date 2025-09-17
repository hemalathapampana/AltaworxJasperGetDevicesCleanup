# AltaworxJasperGetDevicesCleanup Lambda Function - Comprehensive Analysis

## Overview
The **AltaworxJasperGetDevicesCleanup Lambda** function is responsible for processing device synchronization cleanup tasks across multiple carrier integrations including Jasper, ThingSpace, Telegence, eBonding, Teal, and Pond. This Lambda function orchestrates the complete device sync lifecycle from data processing to final notifications.

## 1. SQS Trigger Configuration and Publisher Details

### Primary SQS Queue Configuration
The **AltaworxJasperGetDevicesCleanup Lambda** is triggered by SQS messages from the following queues:

```csharp
// From AltaworxTelegenceAWSGetDeviceUsage namespace
private string ExportDeviceUsageQueueURL = Environment.GetEnvironmentVariable("TelegenceDeviceUsageQueueURL");
private string DeviceNotificationQueueURL = Environment.GetEnvironmentVariable("TelegenceDeviceNotificationQueueURL");
```

### SQS Queue URLs (Environment Variables)
- **Primary Queue**: `DeviceNotificationQueueURL` 
  - Test Environment: `https://sqs.us-east-1.amazonaws.com/130265568833/Jasper_Get_Device_Sync_Notification_TEST`
- **Backup Queue**: `CarrierOptimizationQueueURL`
  - Test Environment: `https://sqs.us-east-1.amazonaws.com/130265568833/CarrierPlanOptimization_TEST`

### SQS Message Publishers
The **AltaworxJasperGetDevicesCleanup Lambda** receives messages from multiple sources:

1. **AltaworxTelegenceAWSGetDeviceUsage Lambda** - Publishes to `TelegenceDeviceUsageQueueURL`
2. **AltaworxJasperGetDevicesCLeanup Lambda** - Publishes to device cleanup queue
3. **Device Usage Processing Lambdas** - Various integration-specific Lambdas

### Expected SQS Message Attributes
```json
{
  "IntegrationType": "1-6", // Integration type ID
  "ServiceProviderId": "integer", // Service provider identifier
  "RetryCount": "integer", // Current retry attempt
  "MaxRetries": "14", // Maximum retry attempts (default)
  "DelayBetweenRetries": "300", // Delay in seconds (default)
  "RemainingRowsToProcess": "integer", // Optional: remaining rows
  "ShouldQueueCarrierOptimization": "boolean", // Optimization flag
  "OptimizationSessionId": "string" // Session ID for optimization
}
```

### Retry Configuration
```csharp
private string DeviceCleanupMaxRetries = Environment.GetEnvironmentVariable("DeviceCleanupMaxRetries");
private const int MaxRetries = 3;
private const int RetryDelaySeconds = 5;
private const int SQSMaxDelaySeconds = 900;
```

## 2. Complete Sequence of Stored Procedures and Dependencies

### Jasper Integration Procedures (IntegrationType: Jasper, POD19, TMobileJasper, Rogers)

#### 2.1 Primary Sync Procedure: `usp_Jasper_DeviceSync`
**Purpose**: Synchronizes Jasper device data with AMOP database
**Parameters**: `@JasperDbName NVARCHAR(50)`, `@ServiceProviderId INT`

**What this procedure does**:
1. **Billing Period Management**: Creates or updates billing periods based on device data
2. **Device Merging**: Merges Jasper devices into the main Device table using batched processing (10,000 records per batch)
3. **Status Updates**: Updates device statuses to 'Unknown' for devices not found in latest sync
4. **Usage Recording**: Creates DeviceUsage records with calculated usage differences

**Key Operations**:
```sql
-- Merge Jasper Devices (Batched Processing)
DECLARE @BatchSize INT = 10000;
DECLARE @MinimumId INT, @MaximumId INT;
SELECT @MinimumId = MIN(ID), @MaximumId = MAX(ID) FROM JasperDevice;

WHILE @MinimumId <= @MaximumId
BEGIN
    MERGE [dbo].[Device] AS TARGET
    USING (SELECT TOP(@BatchSize) ...) AS SOURCE
    -- Complex merge logic with device status and usage updates
END
```

### ThingSpace Integration Procedures

#### 2.2 Primary Sync Procedure: `usp_ThingSpace_DeviceSync`
**Purpose**: Synchronizes ThingSpace (Verizon) device data
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **Unknown Status Handling**: Marks devices with unknown status as inactive
2. **Billing Period Creation**: Creates billing periods for active devices
3. **Device Merging**: Merges ThingSpace devices with complex rate plan disambiguation
4. **Daily Usage Records**: Creates DeviceUsage records from ThingSpaceDeviceDailyUsage

**Key Features**:
- Rate plan disambiguation using ROW_NUMBER() OVER() partitioning
- Handles both ICCID and MSISDN matching
- Creates usage records from daily usage aggregation

### Telegence Integration Procedures (Sequential Execution Required)

#### 2.3.1 Device Update: `usp_Telegence_Update_Device`
**Purpose**: Updates TelegenceDevice table from staging data
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **Device Status Merging**: Updates device status with ROW_NUMBER() partitioning for deduplication
2. **Failed Sync Counting**: Tracks devices that fail sync (max 3 attempts)
3. **Unknown Status Assignment**: Sets status to 'Unknown' after 3 failed syncs
4. **Sync Audit**: Records sync statistics in TelegenceDeviceSyncAudit

#### 2.3.2 Device Detail Update: `usp_Telegence_Update_DeviceDetail`
**Purpose**: Updates device details and creates billing periods
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **Sync Date Logging**: Records sync date in TelegenceDeviceDetailLastSyncDate
2. **Billing Period Creation**: Creates billing periods based on NextBillCycleDate
3. **Customer Billing Periods**: Manages CustomerBillingPeriod table
4. **Device Detail Updates**: Updates device details with billing cycle resets

**Key Logic**:
```sql
-- Billing Cycle Reset Logic
CtdDataUsage = CASE WHEN bp.Id != det.BillingPeriodId  
                THEN 0  
                ELSE CtdDataUsage  
            END,
OldCtdDataUsage = CASE WHEN bp.Id != det.BillingPeriodId  
                    THEN CtdDataUsage  
                    ELSE OldCtdDataUsage  
                END
```

#### 2.3.3 Mobility Feature Update: `usp_Update_TelegenceDeviceMobilityFeature_FromStaging`
**Purpose**: Updates device mobility features from staging
**Parameters**: None

**What this procedure does**:
1. **Feature Mapping**: Maps SOC codes to mobility features
2. **Status Management**: Activates/deactivates features based on staging data
3. **Audit Trail**: Maintains created/modified/deleted timestamps

#### 2.3.4 Usage Update: `usp_Telegence_Update_DeviceUsage_FromStaging`
**Purpose**: Updates device usage from staging tables
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **Usage Aggregation**: Aggregates usage by subscriber, data group, and pool
2. **Usage History**: Maintains TelegenceDeviceUsage_History
3. **Aggregate Management**: Updates MobilityDeviceUsageAggregate for reporting
4. **Pool/Group Linking**: Links devices to usage aggregates

#### 2.3.5 MUBU Usage Update: `usp_Telegence_Update_DeviceMubuUsage_FromStaging`
**Purpose**: Updates device usage from MUBU (Mobile Usage Billing Unit) data
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **MUBU Data Merging**: Merges MUBU staging data into live tables
2. **Usage Calculation**: Combines Kafka and MUBU usage data
3. **Monthly Summaries**: Creates TelegenceDeviceUsageMubuMonthly records
4. **Billing Period Mapping**: Maps usage to correct billing periods

#### 2.3.6 Final Device Sync: `usp_Telegence_DeviceSync`
**Purpose**: Final synchronization of Telegence devices to MobilityDevice table
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **Billing Period Creation**: Creates final billing periods
2. **Pool ID Auto-fill**: Automatically assigns pool IDs based on rate plans
3. **Index Management**: Drops and recreates indexes for performance optimization
4. **Device Merging**: Complex merge with rate plan disambiguation
5. **Usage Record Creation**: Creates DeviceUsage records from MUBU data

### eBonding Integration Procedures

#### 2.4.1 Usage Update: `usp_eBonding_Update_DeviceUsage_FromStaging`
**Purpose**: Updates eBonding device usage from staging
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **Usage Processing**: Processes Data, Voice, and SMS usage separately
2. **Aggregate Management**: Updates MobilityDeviceUsageAggregate
3. **History Maintenance**: Maintains eBondingDeviceUsage_History
4. **Device Linking**: Links devices to usage aggregates

#### 2.4.2 Device Sync: `usp_eBonding_DeviceSync`
**Purpose**: Synchronizes eBonding devices
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **Device Detail Updates**: Updates device details from staging
2. **Mobility Feature Management**: Updates eBondingDeviceMobilityFeature
3. **Billing Period Creation**: Creates billing periods for active devices
4. **Device Merging**: Merges to MobilityDevice with status history
5. **Usage Recording**: Creates DeviceUsage records with calculated differences

### Teal Integration Procedures

#### 2.5 Primary Sync: `usp_Teal_Device_Sync`
**Purpose**: Synchronizes Teal device data
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **Unknown Status Handling**: Manages devices with unknown status
2. **Billing Period Management**: Creates billing periods for active devices
3. **Device Merging**: Merges Teal devices using EID as primary key
4. **Usage Recording**: Creates usage records with data usage calculations
5. **Status Restoration**: Restores active devices that were previously inactive

### Pond Integration Procedures

#### 2.6 Primary Sync: `usp_Pond_Device_Sync`
**Purpose**: Synchronizes Pond device data
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
1. **Unknown Status Management**: Handles devices with unknown status
2. **Billing Period Updates**: Updates billing periods for active devices
3. **Device Merging**: Merges devices with current usage calculations
4. **Usage Recording**: Creates usage records from PondDeviceUsageStaging
5. **Rate Plan Updates**: Updates carrier rate plans from staging data
6. **Historical Corrections**: Updates usage for last closed billing cycles

### Common Post-Processing Procedures

#### 2.7.1 Mobility Device Sync Common: `usp_MobilityDeviceSync_Common`
**Purpose**: Common post-processing for mobility integrations (eBonding, Telegence)
**Parameters**: `@IntegrationId INT`

**What this procedure does**:
1. **Site Management**: Ensures every tenant has default sites
2. **Device Restoration**: Restores archived devices with current usage
3. **Tenant Assignment**: Assigns devices to default sites and tenants
4. **History Management**: Updates MobilityDeviceHistory table
5. **Status Change Tracking**: Records device status changes in DeviceStatusHistory

#### 2.7.2 Device Sync Common: `usp_DeviceSync_Common`
**Purpose**: Common post-processing for M2M integrations (all others)
**Parameters**: `@IntegrationId INT`

**What this procedure does**:
1. **Site Management**: Ensures default sites for all tenants
2. **Device Restoration**: Restores devices with active status
3. **Tenant Linking**: Links devices to appropriate tenants and sites
4. **History Updates**: Updates DeviceHistory table
5. **Status Tracking**: Maintains DeviceStatusHistory for status changes

## 3. New Communication Plans Detection - Exact Query Logic

### Detection Query
The **AltaworxJasperGetDevicesCleanup Lambda** detects new communication plans using this exact SQL logic:

```sql
SELECT DISTINCT d.CommunicationPlan
FROM [dbo].[Device] d
LEFT JOIN [dbo].[JasperCommunicationPlan] jcp 
    ON jcp.CommunicationPlanName = d.CommunicationPlan
    AND jcp.ServiceProviderId = @ServiceProviderId
WHERE d.ServiceProviderId = @ServiceProviderId
  AND jcp.CommunicationPlanName IS NULL
  AND d.CommunicationPlan IS NOT NULL
  AND LTRIM(RTRIM(d.CommunicationPlan)) != ''
```

### How the Detection Works
1. **Left Join Logic**: Performs a LEFT JOIN between Device and JasperCommunicationPlan tables
2. **Null Detection**: Identifies communication plans that exist in Device table but NOT in JasperCommunicationPlan table
3. **Service Provider Filtering**: Ensures detection is scoped to specific service provider
4. **Data Quality Checks**: Excludes NULL and empty communication plans

### Auto-Creation Process
When new communication plans are detected, the **AltaworxJasperGetDevicesCleanup Lambda** automatically creates them:

```sql
INSERT INTO [dbo].[JasperCommunicationPlan] 
(CommunicationPlanName, CreatedBy, CreatedDate, IsActive, IsDeleted, ServiceProviderId)
VALUES (@CommPlan, 'JasperDeviceSync', GETUTCDATE(), 1, 0, @ServiceProviderId)
```

### Notification Process
- **Email Alert**: Sends notification to administrators
- **Subject**: "New Communication Plans Added for [Integration Name]"
- **Content**: Lists all newly detected communication plans
- **Purpose**: Alerts administrators to associate new plans with rate plans for optimization

## 4. Sync Summary Reports - Storage and Details

### Summary Generation Procedures

#### 4.1 ThingSpace Summary: `usp_ThingSpace_Devices_Get_Sync_Summary`
**Purpose**: Generates sync summary for ThingSpace integration
**Parameters**: None

**What this procedure does**:
```sql
SELECT   
  detail.LastSyncDate AS DetailLastSyncDate,   
  detail.QueueCount AS DetailQueueCount,   
  detail_history.UpdatedCount AS DetailUpdatedCount,  
  usage.LastSyncDate AS UsageLastSyncDate,   
  usage.QueueCount AS UsageQueueCount,   
  usage_history.UpdatedCount AS UsageUpdatedCount,  
  device.DeviceCount AS DeviceCount  
FROM
  (SELECT TOP 1 LastSyncDate, QueueCount FROM ThingSpaceDeviceDetailLastSyncDate ORDER BY LastSyncDate DESC) detail
LEFT JOIN 
  (SELECT TOP 1 LastSyncDate, QueueCount FROM ThingSpaceDeviceUsageLastSyncDate ORDER BY LastSyncDate DESC) usage
LEFT JOIN 
  (SELECT COUNT(DISTINCT ICCID) AS UpdatedCount FROM ThingSpaceDeviceDetail_History 
   WHERE CreatedDate >= (SELECT MAX(LastSyncDate) FROM ThingSpaceDeviceDetailLastSyncDate)) detail_history
-- Additional joins for usage history and device count
```

#### 4.2 Telegence Summary: `usp_Telegence_Devices_Get_Sync_Summary`
**Purpose**: Generates sync summary for Telegence integration
**Parameters**: None

**What this procedure does**:
Returns the most recent sync summary from `TelegenceDeviceSyncSummary` table with columns:
- DetailSyncDate, QueuedDetailCount, UpdatedDetailCount
- UsageSyncDate, QueuedUsageCount, UpdatedUsageCount
- DeviceCount

#### 4.3 eBonding Summary: `usp_eBonding_Devices_Get_Sync_Summary`
**Purpose**: Generates sync summary for eBonding integration
**Parameters**: None

**What this procedure does**:
Similar structure to ThingSpace summary but uses eBonding-specific tables:
- eBondingDeviceDetailLastSyncDate
- eBondingDeviceUsageLastSyncDate  
- eBondingDevice_History
- eBondingDeviceUsage_History

#### 4.4 Teal Summary: `usp_Teal_Devices_Get_Sync_Summary`
**Purpose**: Generates sync summary for Teal integration
**Parameters**: None

**What this procedure does**:
Uses Teal-specific tables with EID as primary identifier:
- TealDeviceDetailLastSyncDate
- TealDeviceUsageLastSyncDate
- TealDevice_History
- TealDeviceUsage_History

#### 4.5 Pond Summary: `usp_Pond_Devices_Get_Sync_Summary`
**Purpose**: Generates sync summary for Pond integration
**Parameters**: None

**What this procedure does**:
Uses Pond-specific tables with ICCID as identifier:
- PondDeviceDetailLastSyncDate
- PondDeviceUsageLastSyncDate
- PondDeviceUsageHistory
- PondDevice

#### 4.6 Jasper Summary: `usp_Jasper_Devices_Get_Sync_Summary`
**Purpose**: Generates sync summary for Jasper integration
**Parameters**: `@ServiceProviderId INT`

**What this procedure does**:
Service provider-specific summary using:
- JasperDeviceDetailLastSyncDate
- JasperDeviceUsageLastSyncDate
- JasperDeviceDetail_History
- JasperDeviceUsage_History
- JasperDevice

### Summary Data Processing in Lambda

The **AltaworxJasperGetDevicesCleanup Lambda** processes summary data using the `GetSummaryValues` method:

```csharp
private static DeviceSyncSummary GetSummaryValues(KeySysLambdaContext context, IntegrationType integrationType, int serviceProviderId)
{
    DeviceSyncSummary summary = new DeviceSyncSummary();
    string connectionString = context.CentralDbConnectionString;
    string sqlText = string.Empty;

    switch (integrationType)
    {
        case IntegrationType.ThingSpace:
            sqlText = AMOPConstants.StoredProcedureName.usp_ThingSpace_Devices_Get_Sync_Summary;
            break;
        case IntegrationType.Telegence:
            sqlText = AMOPConstants.StoredProcedureName.usp_Telegence_Devices_Get_Sync_Summary;
            break;
        case IntegrationType.eBonding:
            sqlText = AMOPConstants.StoredProcedureName.usp_eBonding_Devices_Get_Sync_Summary;
            break;
        case IntegrationType.Teal:
            sqlText = AMOPConstants.StoredProcedureName.usp_Teal_Devices_Get_Sync_Summary;
            break;
        case IntegrationType.Jasper:
        case IntegrationType.POD19:
        case IntegrationType.TMobileJasper:
        case IntegrationType.Rogers:
            connectionString = context.GeneralProviderSettings.JasperDbConnectionString;
            sqlText = AMOPConstants.StoredProcedureName.usp_Jasper_Devices_Get_Sync_Summary;
            break;
        case IntegrationType.Pond:
            sqlText = AMOPConstants.StoredProcedureName.usp_Pond_Devices_Get_Sync_Summary;
            break;
    }
    
    // Execute stored procedure and populate DeviceSyncSummary object
}
```

### Storage Locations and Details

#### 4.7 Email Summary Reports
**Configuration**:
- Recipients: `DeviceSyncToEmailAddresses` (semicolon-separated)
- Sender: `DeviceSyncFromEmailAddress`
- Subject: `DeviceSyncResultsEmailSubject` (dynamically replaced with integration name)

**Summary Content Structure**:
```csharp
public class DeviceSyncSummary 
{
    public DateTime? DetailLastSyncDate { get; set; }    // Last device detail sync timestamp
    public int? DetailQueueCount { get; set; }          // Number of device details in queue
    public int? DetailUpdatedCount { get; set; }        // Number of device details updated
    public DateTime? UsageLastSyncDate { get; set; }    // Last usage sync timestamp
    public int? UsageQueueCount { get; set; }           // Number of usage records in queue
    public int? UsageUpdatedCount { get; set; }         // Number of usage records updated
    public int? DeviceCount { get; set; }               // Total device count
}
```

#### 4.8 S3 Storage (Device Sync Summary Logs)
**Configuration**:
- Bucket: `dev-test-daily-usage-s3-bucket` (from `DeviceSyncSummaryLogS3BucketName`)
- Enable Flag: `DeviceSyncSummaryLogEnable = true`
- File Naming Pattern: `{IntegrationType}-{yyyyMMdd}.txt`
- Examples: `Jasper-20241201.txt`, `ThingSpace-20241201.txt`

**S3 Log File Content (CSV Format)**:
```csv
ID,ICCID,MSISDN,RawDataUsage,CurrentDataUsage,RawSMSUsage,CurrentSMSUsage,RawVoiceUsage,CurrentVoiceUsage,BillingCycleStartDate,BillingCycleEndDate,UsageDate,ServiceProviderId
```

**Field Descriptions**:
- `ID`: Device identifier in AMOP system
- `ICCID`: Integrated Circuit Card Identifier (SIM card identifier)
- `MSISDN`: Mobile Station International Subscriber Directory Number (phone number)
- `RawDataUsage`: Raw data usage bytes from carrier API
- `CurrentDataUsage`: Processed data usage bytes in AMOP system
- `RawSMSUsage`: Raw SMS count from carrier API
- `CurrentSMSUsage`: Processed SMS count in AMOP system
- `RawVoiceUsage`: Raw voice usage minutes from carrier API
- `CurrentVoiceUsage`: Processed voice usage minutes in AMOP system
- `BillingCycleStartDate`: Billing period start date
- `BillingCycleEndDate`: Billing period end date
- `UsageDate`: Date when usage occurred
- `ServiceProviderId`: Service provider identifier

#### 4.9 Snowflake Historian Data Export
For non-mobility integrations with Snowflake configuration:
- **Bucket**: `SnowflakeS3BucketName`
- **Path**: `SnowflakeS3BucketPath`
- **File Format**: CSV with complete device history data
- **File Naming**: `DeviceHistory_{ServiceProviderId}_{yyyyMMddhhmmss}.csv`
- **Content**: Complete device historical data for data warehouse ingestion

## 5. AMOP 2.0 Sync Completion Notification

### API Endpoint Configuration
The **AltaworxJasperGetDevicesCleanup Lambda** sends completion notifications to AMOP 2.0 using:

**Environment Variable**: `Amop20SyncUpdateAPIURL`
**Test Environment URL**: `https://demo-api.amop.services/migration_management_uat`

### API Call Implementation
**Service Class**: `DailySyncAmopApiTrigger`
**Method**: `SendNotificationToAmop20()`

```csharp
public void SendNotificationToAmop20(KeySysLambdaContext context, ILambdaContext Lambda_context, string keyName, int? tenantId = null, string tenantName = null)
{
    amop20SyncUpdateApiUrl = AwsFunctionBase.GetStringValueFromEnvironmentVariable(Lambda_context, environmentRepo, CommonConstants.AMOP_20_SYNC_UPDATE_API_URL_KEY);
    
    using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
    {
        client.BaseAddress = new Uri(amop20SyncUpdateApiUrl);
        client.DefaultRequestHeaders.Add("Accept", "application/json");
        
        string jsonRequest = "{\"data\": { \"key_name\": \"" + keyName + "\",\"tenant_id\": \"" + tenantId + "\",\"tenant_name\": \"" + tenantName + "\"}}";
        var contDevice = new StringContent(jsonRequest, Encoding.UTF8, "application/json");
        
        HttpResponseMessage response = client.PostAsync(client.BaseAddress, contDevice).Result;
        
        if (response.IsSuccessStatusCode)
        {
            string responseBody = response.Content.ReadAsStringAsync().Result;
            AwsFunctionBase.LogInfo(context, "SUCCESS", "Sent Response to AMOP2.0");
        }
        else
        {
            var responseBody = response.Content.ReadAsStringAsync().Result;
            AwsFunctionBase.LogInfo(context, "Response Error", responseBody);
        }
    }
}
```

### HTTP Request Details
**Method**: `POST`
**Content-Type**: `application/json`
**Encoding**: `UTF-8`
**Headers**: `Accept: application/json`

### Request Payload Structure
```json
{
  "data": {
    "key_name": "{integrationSpecificKeyName}",
    "tenant_id": "{tenantId}",
    "tenant_name": "{tenantName}"
  }
}
```

### Key Name Mapping by Integration Type
The **AltaworxJasperGetDevicesCleanup Lambda** maps integration types to specific key names:

```csharp
switch (IntegrationId) 
{
    case IntegrationType.ThingSpace:
        keyName = "thingspace_devices";
        break;
    case IntegrationType.Telegence:
        keyName = "telegence_devices";
        break;
    case IntegrationType.eBonding:
        keyName = "ebonding_devices";
        break;
    case IntegrationType.Teal:
        keyName = "teal_devices";
        break;
    case IntegrationType.Jasper:
    case IntegrationType.POD19:
    case IntegrationType.TMobileJasper:
    case IntegrationType.Rogers:
        keyName = "jasper_devices";
        break;
    case IntegrationType.Pond:
        keyName = "pond_inventories";
        break;
    default:
        throw new Exception("Unsupported integration type for AMOP 2.0 notification");
}
```

### Response Handling
- **Success Response**: Logs `"Sent Response to AMOP2.0"` with SUCCESS level
- **Error Response**: Logs complete response body for debugging with "Response Error" level
- **Timeout Handling**: Uses default HttpClient timeout settings
- **Retry Logic**: No automatic retry - single attempt notification

### Notification Timing
The AMOP 2.0 notification is sent by the **AltaworxJasperGetDevicesCleanup Lambda**:
1. **After** all device sync processing completes
2. **After** summary generation and email notifications
3. **After** S3 logging (if enabled)
4. **Regardless** of success or failure status
5. **Before** final cleanup and Lambda termination

This ensures AMOP 2.0 is always notified of sync completion status.

## Complete Process Flow Summary

### Phase 1: Message Reception and Validation
1. **SQS Message Reception** → **AltaworxJasperGetDevicesCleanup Lambda** receives message
2. **Message Parsing** → Extract integration type, service provider ID, retry count
3. **Validation** → Verify message attributes and configuration
4. **Row Count Check** → Verify remaining processing items using database queries

### Phase 2: Integration-Specific Device Synchronization
5. **Device Sync Execution** → Execute integration-specific stored procedures:
   - **Jasper**: `usp_Jasper_DeviceSync` with JasperDbName and ServiceProviderId
   - **ThingSpace**: `usp_ThingSpace_DeviceSync` with ServiceProviderId
   - **Telegence**: Sequential execution of 6 stored procedures
   - **eBonding**: `usp_eBonding_Update_DeviceUsage_FromStaging` → `usp_eBonding_DeviceSync`
   - **Teal**: `usp_Teal_Device_Sync` with ServiceProviderId
   - **Pond**: `usp_Pond_Device_Sync` with ServiceProviderId

### Phase 3: Common Post-Processing
6. **Common Sync Processing** → Execute common procedures:
   - **Mobility Integrations** (eBonding, Telegence): `usp_MobilityDeviceSync_Common`
   - **M2M Integrations** (All others): `usp_DeviceSync_Common`

### Phase 4: Communication Plans and Optimization
7. **Communication Plans Detection** → Execute new communication plans detection query
8. **Auto-Creation** → Create new communication plans if detected
9. **Email Notification** → Send communication plans notification email

### Phase 5: Summary Generation and Reporting
10. **Summary Generation** → Execute integration-specific summary procedures
11. **Email Summary** → Send sync summary email to configured recipients
12. **S3 Logging** → Upload detailed device sync logs (if enabled)
13. **Snowflake Export** → Export device history data (if configured)

### Phase 6: Optimization and Final Notifications
14. **Carrier Optimization** → Queue carrier optimization tasks (if applicable)
15. **AMOP 2.0 Notification** → Send completion notification to AMOP 2.0 API
16. **Retry Logic** → Handle incomplete processing with exponential backoff
17. **Cleanup** → Resource cleanup, logging, and Lambda termination

### Error Handling and Retry Logic
- **Database Timeouts**: Configurable timeouts per stored procedure
- **SQS Retry**: Maximum 14 retries with 300-second delays
- **Exponential Backoff**: Increasing delays between retry attempts
- **Error Logging**: Comprehensive error logging at each phase
- **Partial Failure Handling**: Continue processing even if some phases fail

This comprehensive analysis covers all aspects of the **AltaworxJasperGetDevicesCleanup Lambda** function's operation, dependencies, and integration points across the entire device synchronization ecosystem.