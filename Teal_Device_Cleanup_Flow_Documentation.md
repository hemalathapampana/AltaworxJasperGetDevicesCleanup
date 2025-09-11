# Teal Device Cleanup Flow Documentation

## Overview
This document provides a comprehensive analysis of the Teal-specific device cleanup flow within the AltaworxJasperGetDevicesCleanup system. It traces the complete execution path from the initial SQS trigger through all Teal-specific operations and cleanup procedures.

## Table of Contents
- [Entry Point and Initialization](#entry-point-and-initialization)
- [Teal Integration Detection](#teal-integration-detection)
- [Teal Device Synchronization](#teal-device-synchronization)
- [Common Operations](#common-operations)
- [Summary and Reporting](#summary-and-reporting)
- [External System Integration](#external-system-integration)
- [Error Handling](#error-handling)
- [Configuration Requirements](#configuration-requirements)
- [Performance Considerations](#performance-considerations)

## Entry Point and Initialization

### 1. Lambda Function Handler
```csharp
Function.FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
```
- **Purpose**: Main entry point for AWS Lambda function
- **Input**: SQS event containing Teal device cleanup messages
- **Process**: Initializes KeySysLambdaContext and processes the event

### 2. Base Initialization
```csharp
AwsFunctionBase.BaseFunctionHandler(context) → Returns KeySysLambdaContext
```
- Creates lambda execution context
- Sets up logging infrastructure
- Initializes AWS service connections

### 3. Service Initialization
```csharp
Function.InitializeServices(keysysContext)
```
**Key Operations for Teal Processing:**
- Retrieves S3 bucket configuration for device sync summary logs
- Enables/disables logging based on `DEVICE_SYNC_SUMMARY_LOG_ENABLE` environment variable
- Initializes S3 wrapper for Teal summary file operations
- Sets up Base64 service and settings repository

**Environment Variables Retrieved:**
- `DeviceSyncSummaryLogS3BucketName`
- `DeviceSyncSummaryLogEnable`

## Teal Integration Detection

### 4. Message Processing
```csharp
Function.ProcessEventAsync(keysysContext, sqsEvent)
→ Function.ProcessEventRecordAsync(context, message)
```

### 5. SQS Message Parsing for Teal
```csharp
Function.GetMessageQueueValues(context, message) → Creates GetDevicesCleanupSqsValues
```

**Teal-Specific Message Attributes:**
- `IntegrationType`: Must be "Teal"
- `ServiceProviderId`: Teal service provider identifier
- `RetryCount`: Current retry attempt
- `MaxRetries`: Maximum allowed retries for Teal operations
- `DelayBetweenRetries`: Backoff delay for Teal sync failures
- `RemainingRowsToProcess`: Count of pending Teal device records

### 6. Integration Type Resolution
```csharp
IntegrationTypeRepository.GetIntegrationTypes()
```
- Queries database for all integration types
- Confirms Teal integration type exists and is active
- Used later for email subject customization

### 7. Processing Queue Management
```csharp
Function.CountRowsToProcess(context, integrationType)
Function.IsTooManyRetries(remainingRows, sqsValues)
```

**Teal-Specific Logic:**
- Counts remaining Teal device records to process
- Implements retry logic with exponential backoff
- Determines if Teal sync should proceed or be retried later

## Teal Device Synchronization

### 8. Main Teal Sync Operation
```csharp
Function.SyncDeviceTables(context, sqsValues, sqlRetryPolicy)
→ Function.SyncTealDevices()
```

**Core Teal Sync Process:**
```csharp
Function.SyncTealDevices()
{
    // Execute Teal device synchronization stored procedure
    var parameters = new List<SqlParameter>
    {
        new SqlParameter("@ServiceProviderId", sqsValues.ServiceProviderId)
    };
    
    await ExecuteStoredProcedureAsync(
        "usp_Teal_Device_Sync", 
        parameters, 
        Constants.STANDARD_TIMEOUT
    );
}
```

**Stored Procedure Details:**
- **Name**: `usp_Teal_Device_Sync`
- **Timeout**: Standard timeout (typically 900 seconds)
- **Parameters**: ServiceProviderId
- **Purpose**: Synchronizes Teal device data from staging tables to main device tables

**Database Operations Performed:**
1. **Staging Data Processing**: Processes records from Teal staging tables
2. **Device Information Update**: Updates main Device table with latest Teal data
3. **Usage Data Synchronization**: Syncs usage information from Teal systems
4. **Status Updates**: Updates device status and connectivity information
5. **Data Validation**: Performs data integrity checks
6. **Cleanup Operations**: Removes processed staging records

### 9. Common M2M Sync Operations
```csharp
Function.SyncCommonM2MItems()
```
**Process**: Executes `usp_DeviceSync_Common` stored procedure
- **Purpose**: Standardized post-processing for Teal device data
- **Operations**:
  - Device status normalization
  - Rate plan associations
  - Billing cycle assignments
  - Data usage calculations

### 10. Teal Device Sync Summary Logging
```csharp
// Generate Teal-specific device sync summary
if (deviceSyncSummaryLogEnable)
{
    await GenerateTealDeviceSyncSummary(context, sqsValues);
}
```

**Teal Summary Log Generation:**
- **File Format**: CSV with Teal device usage comparisons
- **Filename Pattern**: `TealDeviceSync_{ServiceProviderId}_{timestamp}.csv`
- **Storage Location**: S3 bucket specified in environment variables

**Summary Columns:**
- Device ID
- ICCID (SIM card identifier)
- MSISDN (phone number)
- Current usage data
- Previous usage data
- Usage delta
- Last sync timestamp
- Billing period information

**SQL Query for Teal Summary:**
```sql
-- Teal-specific device summary query
SELECT 
    d.Id,
    d.ICCID,
    d.MSISDN,
    d.CurrentDataUsage,
    d.PreviousDataUsage,
    d.LastSyncDate,
    d.BillingCycleDate
FROM Device d
WHERE d.ServiceProviderId = @ServiceProviderId
AND d.LastSyncDate >= @SyncStartTime
ORDER BY d.LastSyncDate DESC
```

## Common Operations

### 11. Communication Plans Update
```csharp
Function.UpdateCommPlans(context, sqsValues)
```

**Teal Communication Plans Process:**
1. **Plan Detection**: Identifies new Teal communication plans
```sql
SELECT DISTINCT d.CommunicationPlan
FROM [dbo].[Device] d
LEFT JOIN [dbo].[JasperCommunicationPlan] jcp 
    ON jcp.CommunicationPlanName = d.CommunicationPlan
WHERE d.ServiceProviderId = @ServiceProviderId
    AND jcp.CommunicationPlanName IS NULL
    AND d.CommunicationPlan IS NOT NULL
```

2. **Plan Creation**: Inserts new plans into JasperCommunicationPlan table
3. **Email Notification**: Sends notification about new Teal plans requiring attention

### 12. Email Notification for New Plans
```csharp
Function.SendEmailAsync()
```
- **Subject**: "New Communication Plans Added - Teal"
- **Content**: Lists new Teal communication plans
- **Recipients**: Device sync administrators
- **Purpose**: Alert about plans needing rate plan association

## Summary and Reporting

### 13. Teal Summary Data Collection
```csharp
Function.GetSummaryValues(context, integrationType, serviceProviderId)
```

**Teal Summary Stored Procedure:**
```sql
EXEC usp_Teal_Devices_Get_Sync_Summary @ServiceProviderId
```

**Teal Summary Data Structure:**
```csharp
public class TealSyncSummary
{
    public DateTime? DetailLastSyncDate { get; set; }
    public int DetailQueueCount { get; set; }
    public int DetailUpdatedCount { get; set; }
    public DateTime? UsageLastSyncDate { get; set; }
    public int UsageQueueCount { get; set; }
    public int UsageUpdatedCount { get; set; }
    public int DeviceCount { get; set; }
}
```

### 14. Email Summary Generation
```csharp
Function.SendEmailSummaryAsync(context, integrationType, summary, shouldGoToHistorian, integrationTypes)
```

**Teal Email Summary:**
- **Subject**: "Teal Device Sync Summary - [ServiceProviderName]"
- **Content**: HTML and text versions with Teal sync statistics
- **Recipients**: Configured in GeneralProviderSettings
- **Additional Info**: Snowflake historian status for Teal data

**Email Content Structure:**
```html
<h2>Teal Device Sync Summary</h2>
<table>
    <tr><td>Service Provider:</td><td>[ServiceProviderName]</td></tr>
    <tr><td>Sync Date:</td><td>[SyncDateTime]</td></tr>
    <tr><td>Total Devices:</td><td>[DeviceCount]</td></tr>
    <tr><td>Details Updated:</td><td>[DetailUpdatedCount]</td></tr>
    <tr><td>Usage Records Updated:</td><td>[UsageUpdatedCount]</td></tr>
    <tr><td>Last Detail Sync:</td><td>[DetailLastSyncDate]</td></tr>
    <tr><td>Last Usage Sync:</td><td>[UsageLastSyncDate]</td></tr>
</table>
```

## External System Integration

### 15. Snowflake Integration for Teal
```csharp
Function.SendDeviceHistoryToSnowflake(context, sqsValues)
```

**Teal Snowflake Export:**
- **Trigger**: Teal is a non-mobility integration, so data is exported to Snowflake
- **Data Source**: Device table with Teal tenant information
- **Output Format**: CSV for Snowflake compatibility

**Teal Device History Query:**
```sql
SELECT 
    d.Id as DeviceId,
    d.ICCID,
    d.MSISDN,
    d.DeviceStatus,
    d.ActivationDate,
    d.DeactivationDate,
    d.LastSyncDate,
    sp.Name as ServiceProviderName,
    dt.TenantId,
    dt.TenantName
FROM Device d
INNER JOIN ServiceProvider sp ON d.ServiceProviderId = sp.Id
LEFT JOIN Device_Tenant dt ON d.Id = dt.DeviceId
WHERE d.ServiceProviderId = @ServiceProviderId
    AND d.LastSyncDate >= @SyncDate
```

**File Operations:**
- **Filename**: `DeviceHistory_Teal_{ServiceProviderId}_{timestamp}.csv`
- **Upload Location**: Configured Snowflake S3 bucket
- **Date Format**: ISO format for Snowflake compatibility

### 16. AMOP 2.0 Notification
```csharp
DailySyncAmopApiTrigger.SendNotificationToAmop20(context, lambdaContext, keyName, tenantId, null)
```

**Teal AMOP 2.0 Integration:**
- **Key Mapping**: Teal → "teal_devices"
- **API Endpoint**: Configured AMOP 2.0 API URL
- **Method**: HTTP POST
- **Content-Type**: application/json

**Payload Structure:**
```json
{
    "key_name": "teal_devices",
    "tenant_id": "[TenantId]",
    "tenant_name": "[TenantName]",
    "sync_timestamp": "[ISO8601DateTime]",
    "service_provider_id": "[ServiceProviderId]"
}
```

## Error Handling

### 17. Teal-Specific Error Handling

**SQL Timeout Handling:**
```csharp
try
{
    await SyncTealDevices();
}
catch (SqlException ex) when (ex.Number == -2) // Timeout
{
    // Log timeout error
    context.Logger.LogError($"Teal sync timeout: {ex.Message}");
    
    // Implement retry logic
    if (sqsValues.RetryCount < sqsValues.MaxRetries)
    {
        await RequeueMessage(sqsValues);
    }
}
```

**Teal Sync Error Types:**
1. **Database Connection Errors**: Retry with exponential backoff
2. **Stored Procedure Timeouts**: Log and retry with increased timeout
3. **Data Validation Errors**: Log details and continue processing
4. **S3 Upload Failures**: Retry file operations
5. **Email Notification Failures**: Log but don't fail the sync

**Error Notification:**
- **Recipients**: Device sync administrators
- **Subject**: "Teal Device Sync Error - [ServiceProviderName]"
- **Content**: Error details and troubleshooting information

### 18. Retry Logic for Teal Operations
```csharp
var retryPolicy = Policy
    .Handle<SqlException>()
    .Or<TimeoutException>()
    .WaitAndRetryAsync(
        retryCount: 3,
        sleepDurationProvider: retryAttempt => 
            TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
        onRetry: (outcome, timespan, retryCount, context) =>
        {
            logger.LogWarning($"Teal sync retry {retryCount} after {timespan}s delay");
        }
    );
```

## Configuration Requirements

### Environment Variables
```bash
# Required for Teal processing
DeviceNotificationQueueURL=<SQS_QUEUE_URL>
ConnectionString=<DATABASE_CONNECTION_STRING>
BaseMultiTenantConnectionString=<MULTITENANT_CONNECTION_STRING>
PORTAL_CONNECTION_STRING=<PORTAL_DATABASE_CONNECTION_STRING>

# Teal summary logging
DEVICE_SYNC_SUMMARY_LOG_S3_BUCKET_NAME=<S3_BUCKET_NAME>
DEVICE_SYNC_SUMMARY_LOG_ENABLE=true

# Snowflake integration
SnowflakeS3BucketName=<SNOWFLAKE_S3_BUCKET>
SnowflakeS3BucketPath=<SNOWFLAKE_S3_PATH>

# AMOP 2.0 integration
AMOP_20_SYNC_UPDATE_API_URL_KEY=<AMOP_API_URL>
```

### Database Configuration

**Required Tables:**
- `Device`: Main device information
- `ServiceProvider`: Teal service provider details
- `Integration`: Integration type definitions
- `JasperCommunicationPlan`: Communication plan mappings
- `GeneralProviderSettings`: Email and notification settings
- `Device_Tenant`: Multi-tenant device associations

**Required Stored Procedures:**
- `usp_Teal_Device_Sync`: Main Teal synchronization
- `usp_DeviceSync_Common`: Common M2M operations
- `usp_Teal_Devices_Get_Sync_Summary`: Summary generation

**ServiceProviderSetting Configurations:**
```sql
-- Email settings for Teal notifications
INSERT INTO ServiceProviderSetting (ServiceProviderId, SettingName, SettingValue)
VALUES 
    (@TealServiceProviderId, 'DeviceSyncFromEmail', 'noreply@company.com'),
    (@TealServiceProviderId, 'DeviceSyncToEmail', 'admin@company.com'),
    (@TealServiceProviderId, 'DeviceSyncEmailSubject', 'Teal Device Sync Summary');
```

### AWS Service Configuration

**SQS Queue Setup:**
- Dead letter queue for failed Teal messages
- Visibility timeout: 900 seconds (15 minutes)
- Message retention: 14 days
- Redrive policy: 3 attempts before DLQ

**S3 Bucket Permissions:**
- Read/write access for summary logs
- Cross-region replication for Snowflake buckets
- Lifecycle policies for log retention

**SES Configuration:**
- Verified sender addresses for notifications
- Email templates for Teal-specific messages
- Bounce and complaint handling

## Performance Considerations

### Timeout Management
```csharp
// Teal-specific timeouts
public static class TealTimeouts
{
    public const int DEVICE_SYNC_TIMEOUT = 900; // 15 minutes
    public const int COMMON_SYNC_TIMEOUT = 300; // 5 minutes
    public const int SUMMARY_GENERATION_TIMEOUT = 180; // 3 minutes
}
```

### Batch Processing Optimization
- **Device Batch Size**: Process Teal devices in batches of 1000
- **SQL Parameter Optimization**: Use table-valued parameters for bulk operations
- **Connection Pooling**: Reuse database connections across operations
- **Memory Management**: Stream large result sets to avoid memory issues

### Monitoring and Metrics
```csharp
// Teal-specific performance metrics
public class TealSyncMetrics
{
    public TimeSpan SyncDuration { get; set; }
    public int DevicesProcessed { get; set; }
    public int ErrorCount { get; set; }
    public long MemoryUsage { get; set; }
    public int RetryAttempts { get; set; }
}
```

### Resource Management
```csharp
// Proper resource disposal for Teal operations
using (var connection = new SqlConnection(connectionString))
using (var command = new SqlCommand("usp_Teal_Device_Sync", connection))
{
    command.CommandType = CommandType.StoredProcedure;
    command.CommandTimeout = TealTimeouts.DEVICE_SYNC_TIMEOUT;
    
    // Execute Teal sync operation
    await command.ExecuteNonQueryAsync();
}
```

## Cleanup and Finalization

### Resource Cleanup
```csharp
// Final cleanup operations for Teal processing
try
{
    // Flush logs and release resources
    keysysContext?.CleanUp();
    
    // Close database connections
    await connection.DisposeAsync();
    
    // Clear memory caches
    GC.Collect();
}
catch (Exception ex)
{
    logger.LogError($"Teal cleanup error: {ex.Message}");
}
```

### Success Confirmation
- Log successful completion of Teal sync
- Update processing status in database
- Send success notification to monitoring systems
- Clear any temporary files or resources

---

## Conclusion

This documentation provides a comprehensive overview of the Teal device cleanup flow within the AltaworxJasperGetDevicesCleanup system. The Teal integration follows the standard device sync pattern while incorporating Teal-specific stored procedures, error handling, and reporting mechanisms.

Key characteristics of the Teal flow:
- Uses standard M2M sync operations
- Exports data to Snowflake for analytics
- Implements robust error handling and retry logic
- Generates comprehensive summary reports
- Integrates with AMOP 2.0 notification system

For troubleshooting Teal sync issues, check the CloudWatch logs, S3 summary files, and database stored procedure execution plans.