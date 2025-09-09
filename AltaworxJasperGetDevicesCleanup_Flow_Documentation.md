# AltaworxJasperGetDevicesCleanup - Complete Flow Documentation

## Overview
This document provides a comprehensive analysis of the device cleanup flow starting from `AltaworxJasperGetDevicesCleanup.cs` and traces through all internal methods across the associated classes.

## Sequential Function Flow (Start to End)

### 1. Entry Point
- **`Function.FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`**

### 2. Base Initialization
- **`AwsFunctionBase.BaseFunctionHandler(context)`** → Returns `KeySysLambdaContext`
- **`Function.InitializeServices(keysysContext)`**
  - **`AwsFunctionBase.GetStringValueFromEnvironmentVariable()`** (DeviceSyncSummaryLogS3BucketName)
  - **`AwsFunctionBase.GetBooleanValueFromEnvironmentVariable()`** (DeviceSyncSummaryLogEnable)
  - **`SettingsRepository.GetGeneralProviderSettings()`** (if logging enabled)

### 3. Main Processing
- **`Function.ProcessEventAsync(keysysContext, sqsEvent)`**
  - **`Function.ProcessEventRecordAsync(context, message)`**
    - **`Function.GetMessageQueueValues(context, message)`** → Creates `GetDevicesCleanupSqsValues`
    - **`IntegrationTypeRepository.GetIntegrationTypes()`**
    - **`Function.CountRowsToProcess(context, integrationType)`**
    - **`Function.IsTooManyRetries(remainingRows, sqsValues)`**

### 4. Device Synchronization Flow
- **`Function.SyncDeviceTables(context, sqsValues, sqlRetryPolicy)`**
  - Based on IntegrationType, calls specific sync methods:
    - **ThingSpace**: `Function.SyncThingSpaceDevices()`
    - **Telegence**: Multiple methods in sequence:
      - `Function.UpdateTelegenceDevicesFromStaging()`
      - `Function.UpdateTelegenceDeviceDetailsFromStaging()`
      - `Function.UpdateMobilityFeatureFromStaging()`
      - `Function.UpdateTelegenceUsageFromStaging()`
      - `Function.UpdateTelegenceMubuUsageFromStaging()`
      - `Function.RunTelegenceDeviceSync()`
    - **eBonding**: 
      - `Function.UpdateEbondingUsage()`
      - `Function.SyncEbondingDevices()`
    - **Teal**: `Function.SyncTealDevices()`
    - **Jasper/POD19/TMobileJasper/Rogers**: `Function.SyncJasperDevices()`
    - **Pond**: `Function.SyncPondDevices()`
  - **Common Sync Operations**:
    - For Mobility: `Function.SyncCommonMobilityItems()`
    - For M2M: `Function.SyncCommonM2MItems()`

### 5. Communication Plans Update
- **`Function.UpdateCommPlans(context, sqsValues)`**
- **`Function.SendEmailAsync()`** (if new plans added)

### 6. Summary Generation
- **`Function.GetSummaryValues(context, integrationType, serviceProviderId)`**
- **`Function.SendEmailSummaryAsync(context, integrationType, summary, shouldGoToHistorian, integrationTypes)`**

### 7. Snowflake Integration (if configured)
- **`Function.SendDeviceHistoryToSnowflake(context, sqsValues)`**
  - **`Function.GetDeviceHistoryString(context, sqsValues)`**

### 8. Carrier Optimization (for specific integration types)
- **`Function.SendCarrierOptimizationMessageToQueue(context, serviceProviderId, optimizationSessionId)`**

### 9. ThingSpace Discrepancy Check
- **`ThingSpaceDiscrepancyService.CheckDevicesForBillingPeriodDiscrepancy(context, sqsValues)`**
  - **`ThingSpaceDiscrepancyService.GetDevicesWithBillingCycleDiscrepancy()`**

### 10. AMOP 2.0 Notification
- **`DailySyncAmopApiTrigger.SendNotificationToAmop20(context, lambdaContext, keyName, tenantId, null)`**

### 11. Error Handling
- **`OptimizationUsageSyncErrorHandler.ProcessStopCarrierOptimization()`** (if errors occur)

## Detailed Low-Level Flow for Each Section

### A. Initialization Section

#### A.1 Function Handler Entry
- **Purpose**: Main entry point for Lambda function
- **Input**: SQS event with device cleanup messages
- **Process**:
  - Creates `KeySysLambdaContext` using base function handler
  - Initializes services and environment variables
  - Handles queue URL configuration
  - Processes the SQS event
- **Error Handling**: Try-catch with Lambda logger fallback

#### A.2 Service Initialization
- **Purpose**: Setup required services and configurations
- **Key Operations**:
  - Retrieves S3 bucket configuration for device sync summary logs
  - Enables/disables logging based on environment variables
  - Initializes S3 wrapper for file operations
  - Sets up Base64 service and settings repository
- **Dependencies**: `EnvironmentRepository`, `SettingsRepository`, `Base64Service`

### B. Message Processing Section

#### B.1 SQS Message Parsing
- **Purpose**: Extract and validate message attributes from SQS
- **Key Attributes Extracted**:
  - RetryCount, RemainingRowsToProcess, MaxRetries
  - DelayBetweenRetries, IntegrationType, ServiceProviderId
  - ShouldQueueCarrierOptimization, OptimizationSessionId
  - Snowflake S3 bucket configuration
- **Validation**: Checks for required integration type

#### B.2 Integration Type Resolution
- **Purpose**: Get all available integration types from database
- **Process**:
  - Queries Integration table for ID and Name mapping
  - Used later for email subject customization
- **SQL**: `SELECT id, Name FROM Integration`

#### B.3 Processing Queue Management
- **Purpose**: Determine if cleanup process should proceed or retry
- **Logic**:
  - Counts remaining rows to process for specific integration type
  - Checks retry limits and previous processing state
  - For eBonding: Special handling for unchanged row counts
  - Implements exponential backoff for retries

### C. Device Synchronization Section

#### C.1 Integration-Specific Sync Methods

##### C.1.1 ThingSpace Sync
- **Stored Procedure**: `usp_ThingSpace_DeviceSync`
- **Timeout**: 900 seconds
- **Process**: Synchronizes device data from ThingSpace staging tables
- **Summary Log**: Generates usage comparison data

##### C.1.2 Telegence Sync (Multi-step process)
- **Step 1**: `usp_Telegence_Update_Device` - Updates device information
- **Step 2**: `usp_Telegence_Update_DeviceDetail` - Updates device details
- **Step 3**: `usp_Update_TelegenceDeviceMobilityFeature_FromStaging` - Updates mobility features
- **Step 4**: `usp_Telegence_Update_DeviceUsage_FromStaging` - Updates usage data
- **Step 5**: `usp_Telegence_Update_DeviceMubuUsage_FromStaging` - Updates MUBU usage
- **Step 6**: `usp_Telegence_DeviceSync` - Final device synchronization
- **Timeout**: 1800 seconds for most operations

##### C.1.3 eBonding Sync
- **Step 1**: `usp_eBonding_Update_DeviceUsage_FromStaging` - Updates usage data
- **Step 2**: `usp_eBonding_DeviceSync` - Synchronizes device information
- **Timeout**: 60-90 seconds

##### C.1.4 Teal Sync
- **Stored Procedure**: `usp_Teal_Device_Sync`
- **Timeout**: Standard timeout (from constants)
- **Process**: Synchronizes Teal device data

##### C.1.5 Jasper/POD19/TMobile/Rogers Sync
- **Stored Procedure**: `usp_Jasper_DeviceSync`
- **Parameters**: JasperDbName, ServiceProviderId
- **Process**: Cross-database sync with transaction management
- **Special Handling**: Uses separate Jasper database connection

##### C.1.6 Pond Sync
- **Stored Procedure**: `POND_DEVICE_SYNC`
- **Process**: Synchronizes Pond inventory data
- **Timeout**: Standard timeout

#### C.2 Common Sync Operations
- **Mobility Items**: `usp_MobilityDeviceSync_Common` (for eBonding, Telegence)
- **M2M Items**: `usp_DeviceSync_Common` (for other integration types)
- **Purpose**: Standardized post-processing for device data

#### C.3 Device Sync Summary Logging
- **Purpose**: Generate detailed logs of sync operations
- **File Format**: CSV with device usage comparisons
- **Columns**: ID, ICCID, MSISDN, Raw/Current usage data, billing dates
- **Storage**: S3 bucket with timestamped filenames
- **Integration-Specific Queries**: Different SQL for each integration type

### D. Communication Plans Section

#### D.1 New Plan Detection
- **Purpose**: Identify communication plans not in JasperCommunicationPlan table
- **SQL Query**:
  ```sql
  SELECT DISTINCT d.CommunicationPlan
  FROM [dbo].[Device] d
  LEFT JOIN [dbo].[JasperCommunicationPlan] jcp ON jcp.CommunicationPlanName = d.CommunicationPlan
  WHERE d.ServiceProviderId = @ServiceProviderId
  AND jcp.CommunicationPlanName IS NULL
  AND d.CommunicationPlan IS NOT NULL
  ```

#### D.2 Plan Creation
- **Process**: Insert new plans into JasperCommunicationPlan table
- **Default Values**: CreatedBy='JasperDeviceSync', IsActive=1, IsDeleted=0
- **Notification**: Sends email notification about new plans added

#### D.3 Email Notification
- **Subject**: "New Communication Plans Added"
- **Content**: HTML and text versions listing new plans
- **Purpose**: Alert administrators about plans needing rate plan association

### E. Summary and Reporting Section

#### E.1 Summary Data Collection
- **Purpose**: Generate sync statistics for reporting
- **Integration-Specific Procedures**:
  - ThingSpace: `usp_ThingSpace_Devices_Get_Sync_Summary`
  - Telegence: `usp_Telegence_Devices_Get_Sync_Summary`
  - eBonding: `usp_eBonding_Devices_Get_Sync_Summary`
  - Teal: `usp_Teal_Devices_Get_Sync_Summary`
  - Jasper variants: `usp_Jasper_Devices_Get_Sync_Summary`
  - Pond: `usp_Pond_Devices_Get_Sync_Summary`

#### E.2 Summary Data Structure
- **Fields**:
  - DetailLastSyncDate, DetailQueueCount, DetailUpdatedCount
  - UsageLastSyncDate, UsageQueueCount, UsageUpdatedCount
  - DeviceCount
- **Purpose**: Track sync performance and data volumes

#### E.3 Email Summary Generation
- **Recipients**: Configured in GeneralProviderSettings
- **Subject**: Dynamic based on integration type
- **Content**: HTML and text versions with sync statistics
- **Additional Info**: Snowflake historian status

### F. Snowflake Integration Section

#### F.1 Historian Data Export
- **Trigger**: Non-mobility integrations with configured S3 bucket
- **Data Source**: Device table with tenant information
- **SQL Query**: Complex join across Device, ServiceProvider, Device_Tenant tables
- **Output**: CSV format with device history data

#### F.2 Data Transformation
- **Date Formatting**: ISO format for Snowflake compatibility
- **File Naming**: `DeviceHistory_{ServiceProviderId}_{timestamp}.csv`
- **Upload**: Direct to S3 bucket using configured path

### G. Carrier Optimization Section

#### G.1 Optimization Triggering
- **Conditions**: 
  - Jasper-family integrations (Jasper, POD19, TMobile, Rogers)
  - ShouldQueueCarrierOptimization flag set
- **Process**: Queues optimization request for carrier

#### G.2 Billing Period Resolution
- **Primary**: Current month billing period
- **Fallback**: Next month billing period (for new service providers)
- **Error Handling**: Logs exception if no billing period found

#### G.3 SQS Message Creation
- **Queue**: CarrierOptimizationQueueURL
- **Attributes**: BillYear, BillMonth, ServiceProviderId, TenantId, BillPeriodId, HasSynced
- **Optional**: OptimizationSessionId (if provided)
- **Body**: Descriptive message about billing period

### H. ThingSpace Discrepancy Section

#### H.1 Discrepancy Detection
- **Trigger**: Only for ThingSpace integration type
- **Purpose**: Identify devices with incorrect billing cycle assignments
- **Stored Procedure**: `usp_GetDevicesWithBillingCycleDiscrepancy`

#### H.2 Report Generation
- **Format**: Excel file (.xlsx) using EPPlus library
- **Worksheet**: "IncorrectBillCycleDevices"
- **Content**: Device list with discrepancy details
- **Email Attachment**: Sent to configured recipients

#### H.3 Email Service
- **Service**: DeviceDiscrepancyEmailService
- **Recipients**: Device sync email addresses
- **Subject**: Billing period discrepancy notification
- **Attachment**: Excel file with device details

### I. AMOP 2.0 Integration Section

#### I.1 Notification Trigger
- **Purpose**: Notify AMOP 2.0 system of completion
- **Key Mapping**: Integration type to key name mapping
  - ThingSpace → "thingspace_devices"
  - Telegence → "telegence_devices"
  - Teal → "teal_devices"
  - Jasper family → "jasper_devices"
  - Pond → "pond_inventories"

#### I.2 API Communication
- **Method**: HTTP POST to configured AMOP 2.0 API endpoint
- **Content-Type**: application/json
- **Payload**: JSON with key_name, tenant_id, tenant_name
- **Logging**: Success/failure response logging

### J. Error Handling Section

#### J.1 Carrier Optimization Error Handling
- **Trigger**: Exception during processing with pending optimization
- **Process**: 
  - Calls `OptimizationUsageSyncErrorHandler.ProcessStopCarrierOptimization()`
  - Removes optimization session from database
  - Sends error notification email

#### J.2 Email Error Notifications
- **Service**: Uses same email infrastructure as summary emails
- **Content**: Error details and service provider information
- **Recipients**: Device sync email addresses
- **Subject**: Carrier optimization sync error

#### J.3 Retry Logic
- **SQL Retry**: Uses Polly retry policies for transient SQL errors
- **SQS Retry**: Implements exponential backoff for queue retries
- **Timeout Handling**: Treats SQL timeouts as retryable errors
- **Max Retries**: Configurable per integration type

### K. Cleanup and Finalization

#### K.1 Context Cleanup
- **Process**: `keysysContext?.CleanUp()`
- **Purpose**: Flush logs and release resources
- **Location**: Finally block and normal completion

#### K.2 Queue Management
- **Success**: No re-queuing required
- **Retry Needed**: Sends message back to queue with updated retry count
- **Failure**: Error logging and potential optimization session cleanup

## Key Dependencies and External Systems

### Database Systems
- **Central Database**: Primary AMOP database
- **Jasper Database**: Separate database for Jasper integrations
- **Portal Database**: For tenant information lookup

### AWS Services
- **SQS**: Message queuing for processing coordination
- **S3**: File storage for summary logs and Snowflake exports
- **SES**: Email service for notifications

### External APIs
- **AMOP 2.0 API**: Notification endpoint for process completion

### Third-Party Libraries
- **EPPlus**: Excel file generation
- **MimeKit**: Email message construction
- **Polly**: Retry policy implementation
- **StackExchange.Redis**: Caching (context-dependent)

## Configuration Requirements

### Environment Variables
- DeviceNotificationQueueURL, CarrierOptimizationQueueURL
- ConnectionString, BaseMultiTenantConnectionString
- SnowflakeS3BucketName, SnowflakeS3BucketPath
- DEVICE_SYNC_SUMMARY_LOG_S3_BUCKET_NAME, DEVICE_SYNC_SUMMARY_LOG_ENABLE
- AMOP_20_SYNC_UPDATE_API_URL_KEY
- PORTAL_CONNECTION_STRING

### Database Settings (ServiceProviderSetting table)
- Email configuration (from/to addresses, subjects)
- AWS credentials (access keys, secret keys)
- Jasper database connection string
- Integration-specific FTP/API settings

### Integration-Specific Configurations
- Each integration type has specific stored procedures
- Timeout configurations vary by integration complexity
- Retry policies adapted to integration characteristics

## Performance Considerations

### Timeout Management
- Short operations: 60-90 seconds
- Standard operations: 900 seconds (15 minutes)
- Complex operations (Telegence): 1800 seconds (30 minutes)

### Batch Processing
- SQL bulk operations where possible
- Retry policies for transient failures
- Queue-based processing for scalability

### Resource Management
- Connection pooling for database operations
- Proper disposal of resources in using statements
- Memory-efficient file operations for large datasets

This comprehensive documentation covers the complete flow from the initial SQS trigger through all internal method calls, database operations, external service integrations, and final cleanup operations.