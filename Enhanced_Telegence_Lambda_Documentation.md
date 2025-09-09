# Altaworx Telegence AWS Get Device Usage Lambda Flow Documentation - Enhanced

## Overview
This document provides a comprehensive analysis of the AltaworxTelegenceAWSGetDeviceUsage.cs Lambda function and its supporting classes, detailing the complete flow from initialization to completion with detailed process descriptions for each method.

## High-Level Sequential Flow

### 1. Lambda Entry Point

**Method:** `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`

**Detailed Process:**
1. **Context Initialization:**
   - Creates a new `KeySysLambdaContext` instance by calling `BaseFunctionHandler()`
   - Initializes logging infrastructure with caller information tracking
   - Establishes database connections to central and Jasper databases
   - Loads organizational unit (OU) specific settings if not skipped
   - Sets up Base64 decoding service for encrypted configuration values

2. **Environment Configuration Loading:**
   - Retrieves environment variables using `EnvironmentRepository.GetEnvironmentVariable()`
   - Validates required configuration parameters existence
   - Sets default values for optional configurations using null coalescing operators
   - Logs configuration values for debugging purposes (sensitive data excluded)

3. **Settings Initialization:**
   - Calls `SettingsRepository.GetGeneralProviderSettings()` to load system-wide settings
   - Decodes encrypted AWS credentials using Base64 service
   - Establishes AWS SES credentials for email notifications
   - Validates database connection strings and timeout settings

4. **Main Processing Execution:**
   - Invokes `ProcessEventAsync()` with the loaded context and SQS event
   - Wraps execution in try-catch block for comprehensive error handling
   - Tracks execution time and resource utilization metrics

5. **Cleanup and Resource Management:**
   - Calls `CleanUp()` method to dispose of database connections
   - Releases memory allocated for large data structures
   - Logs execution completion status and performance metrics
   - Returns appropriate HTTP status codes based on processing results

### 2. Event Processing

**Method:** `ProcessEventAsync(KeySysLambdaContext context, SQSEvent sqsEvent)`

**Detailed Process:**

**Decision Logic Analysis:**
- **SQS Records Present Path:**
  1. Iterates through `sqsEvent.Records` collection using foreach loop
  2. For each record, extracts message body and attributes
  3. Validates message format and required attributes presence
  4. Calls `ProcessEventRecordAsync()` for individual record processing
  5. Tracks processing success/failure counts for batch reporting
  6. Implements partial failure handling for batch processing scenarios

- **No SQS Records Path (CloudWatch Trigger):**
  1. Detects empty or null records collection
  2. Logs CloudWatch event trigger identification
  3. Sets `fromCloudwatchEvent` flag to true
  4. Calls `StartDailyDeviceUsageProcessingAsync()` for initialization
  5. Implements global processing lock to prevent concurrent executions

**Error Handling:**
- Implements retry logic with exponential backoff for transient failures
- Logs detailed error information including stack traces
- Sends failure notifications to configured email addresses
- Updates processing status in database for monitoring purposes

### 3. Record Processing

**Method:** `ProcessEventRecordAsync(KeySysLambdaContext context, SQSEvent.SQSMessage message)`

**Detailed Process:**

**Message Attribute Extraction:**
1. **Attribute Parsing Process:**
   - Iterates through `message.MessageAttributes` dictionary
   - Converts string values to appropriate data types using type-safe parsing
   - Implements null coalescing for optional attributes with default values
   - Validates attribute value formats using regex patterns where applicable

2. **Extracted Attributes Processing:**
   - `InitializeProcessing`: Parsed as boolean using `bool.TryParse()`
   - `ServiceProviderId`: Converted to integer with validation for positive values
   - `FAN`: String validation for Foundation Account Number format
   - `ReportType`: Enum validation against allowed values (Premier/MUBU/Final)
   - `IsFromCloudwatchEvent`: Boolean flag for trigger source identification
   - `IsDownLoadFileAgain`: Retry flag for failed download processing
   - `IsDownloadNextInstance`: Continuation flag for large file processing
   - `WriteTimesNextDownload`: CSV parsing into List<string> with timestamp validation
   - `FileNamesNextDownload`: CSV parsing into List<string> with filename validation
   - `DownloadFailedIds`: CSV parsing into List<int> with ID validation
   - `TelegenceSyncDataStep`: Enum parsing with step validation

**Processing Logic Branching:**
1. **Initialization Branch (`InitializeProcessing = true`):**
   - Validates no conflicting attributes are set
   - Calls `StartDailyDeviceUsageProcessingAsync()` with CloudWatch flag
   - Logs initialization trigger source and timestamp

2. **MUBU Synchronization Branch:**
   - Validates `TelegenceSyncDataStep` enum value is within expected range
   - Ensures `ServiceProviderId` is valid and exists in database
   - Calls `ProcessTelegenceMubuUsageDataSync()` with step parameter
   - Updates processing status in tracking tables

3. **Standard Processing Branch:**
   - Validates all required attributes for daily processing are present
   - Constructs parameter object with extracted values
   - Calls `ProcessDailyUsage()` with comprehensive parameter set
   - Implements timeout monitoring for long-running operations

## Detailed Method Flows

### A. Daily Usage Processing Flow

**Method:** `ProcessDailyUsage(KeySysLambdaContext context, int serviceProviderId, string fan, string reportType, ...)`

**Detailed Process:**

#### 1. Settings Retrieval Phase:
```csharp
// Process Detail:
1. Database Connection Establishment:
   - Opens SQL connection using context.CentralDbConnectionString
   - Sets connection timeout to SQLConstant.ConnectionTimeoutSeconds
   - Implements connection pooling for performance optimization

2. Settings Query Execution:
   - Executes parameterized query against ServiceProviderSetting table
   - Filters by ServiceProviderId and IsDeleted = 0 for active settings only
   - Uses SqlDataReader for efficient memory usage during data retrieval

3. Credential Processing:
   - Retrieves encrypted FTP password from database
   - Decodes using context.Base64Service.Base64Decode() method
   - Validates credential format and required fields presence
   - Logs successful credential retrieval (excluding sensitive data)

4. Configuration Validation:
   - Validates FTP server connectivity using test connection
   - Checks path accessibility and permissions
   - Verifies MUBU and Final Usage path configurations
   - Implements fallback configuration loading for missing settings
```

#### 2. Processing Branch Selection:

**Download Failed Files Branch:**
```csharp
// ProcessDownloadFileAgain() Internal Process:
1. Failed Files Identification:
   - Queries TelegenceSFTPFileDownloadStatus table for failed downloads
   - Filters by ServiceProviderId and FailureCount < MaxRetryAttempts
   - Orders by FailureTimestamp for chronological retry processing

2. Retry Logic Implementation:
   - Implements exponential backoff calculation: delay = baseDelay * Math.Pow(2, attemptNumber)
   - Updates retry attempt counter in database before processing
   - Tracks cumulative failure reasons for pattern analysis

3. File Re-download Process:
   - Establishes new SFTP connection with fresh credentials
   - Navigates to original file location using stored path information
   - Implements resume capability for partially downloaded files
   - Validates file integrity using checksum comparison

4. Status Update Process:
   - Updates download status to 'Retry_In_Progress' before attempt
   - Records successful download with completion timestamp
   - Logs failure details with specific error codes for monitoring
   - Sends notification emails for repeated failures exceeding threshold
```

**Download Next Instance Branch:**
```csharp
// ProcessDownloadFileNextInstance() Internal Process:
1. Continuation Context Restoration:
   - Retrieves processing state from TelegenceProcessingState table
   - Reconstructs file list and current position markers
   - Validates continuation parameters for data consistency

2. File Queue Management:
   - Parses FileNamesNextDownload CSV into ordered queue
   - Validates file existence and accessibility on SFTP server
   - Implements priority ordering based on file timestamps and types

3. Batch Processing Logic:
   - Processes files in batches defined by LimitAmountFilePerRunTimes
   - Implements memory management for large file processing
   - Tracks processing progress with database checkpoints

4. Completion Detection:
   - Monitors queue depletion and processing completion
   - Updates final processing status in tracking tables
   - Triggers next phase processing via SQS message queuing
```

**Normal Processing Path:**
```csharp
// Main ProcessDailyUsage() Internal Process:
1. Resource Allocation:
   - Allocates memory pools for data processing operations
   - Establishes database connection pools for parallel operations
   - Initializes temporary storage locations for file processing

2. Processing Coordination:
   - Coordinates between FTP cleanup, data processing, and queue management
   - Implements transaction boundaries for data consistency
   - Manages concurrent access to shared resources using locks

3. Error Recovery:
   - Implements checkpoint-based recovery for interrupted processing
   - Maintains processing state in database for resume capability
   - Provides rollback mechanisms for failed operations
```

#### 3. FTP Cleanup Operations:

**CleanUpFtp() Internal Process:**
```csharp
1. Connection Management:
   - Establishes SFTP connection using Renci.SshNet library
   - Implements connection retry logic with exponential backoff
   - Validates server certificate and authentication credentials

2. File Age Calculation:
   - Retrieves file timestamps using SFTP STAT command
   - Calculates age using TimeZone-aware date comparison
   - Applies DaysToKeep configuration for retention policy

3. Cleanup Execution:
   - Identifies files older than retention threshold
   - Implements safe deletion with backup verification
   - Logs deleted file details for audit trail maintenance

4. Space Management:
   - Monitors available disk space on SFTP server
   - Implements emergency cleanup for space-critical situations
   - Reports storage utilization metrics for capacity planning
```

### B. Report Type Processing Details

#### Premier Reports Processing:

**Method:** `GetLatestUsage()` - Internal Process:
```csharp
1. File Discovery Phase:
   - Scans SFTP directory using pattern matching for Premier usage files
   - Filters files by naming convention: *_usage_YYYYMMDD.csv
   - Sorts files by modification timestamp for chronological processing
   - Validates file accessibility and read permissions

2. File Download and Validation:
   - Downloads files using streaming to minimize memory usage
   - Implements checksum validation for data integrity
   - Performs preliminary format validation using header row analysis
   - Logs download metrics including transfer speed and file size

3. Data Parsing and Transformation:
   - Parses CSV files using CsvHelper library with custom mapping
   - Implements data type conversion with error handling
   - Applies business rules for data validation and cleansing
   - Handles encoding issues and special characters in data

4. Staging Table Population:
   - Creates DataTable structure matching TelegenceAllUsageStaging schema
   - Populates DataTable with parsed and validated data
   - Implements batch processing for large datasets
   - Applies column mapping for SqlBulkCopy operation

5. Bulk Copy Execution:
   - Configures SqlBulkCopy with optimized batch size and timeout
   - Implements transaction boundaries for data consistency
   - Monitors memory usage during bulk operations
   - Handles constraint violations and data type mismatches
```

#### MUBU Reports Processing:

**Comprehensive MUBU Processing Flow:**
```csharp
1. Initialization Phase:
   - Creates tracking data structures for file processing state
   - Initializes concurrent collections for thread-safe operations
   - Sets up progress monitoring and cancellation token support

2. Failed Files Recovery:
   // GetFileNamesDownloadFailed() Process:
   - Queries TelegenceSFTPFileDownloadStatus for ServiceProviderId
   - Filters by FailureCount < MaxRetryAttempts configuration
   - Constructs retry queue with priority based on failure age
   - Implements deduplication logic for duplicate failure records

3. Downloaded Files Tracking:
   // GetFilesDownLoaded() Process:
   - Queries TelegenceFileProcessingHistory for completed downloads
   - Creates hash set for O(1) lookup performance during processing
   - Handles file name variations and case sensitivity issues
   - Implements cache invalidation for stale tracking data

4. SFTP Connection Management:
   - Establishes persistent SFTP connection with keep-alive settings
   - Implements connection pooling for multiple concurrent operations
   - Handles network interruptions with automatic reconnection
   - Monitors connection health and performance metrics

5. Voice Files Processing:
   // GetLatestMubuVoiceFileList() Internal Process:
   a. Directory Scanning:
      - Navigates to voice files directory using configured path
      - Lists files matching voice file naming pattern
      - Filters by modification date using configurable threshold
      - Sorts files chronologically for sequential processing

   b. File Selection Logic:
      - Excludes already processed files using downloaded files hash set
      - Applies file size validation to detect incomplete uploads
      - Implements priority selection for recent files
      - Handles file name encoding and special character issues

   // GetLatestMubuVoice() Internal Process:
   a. File Download:
      - Downloads voice files using streaming with progress callbacks
      - Implements resume capability for interrupted downloads
      - Validates file integrity using size and checksum verification
      - Handles timeout scenarios with configurable retry attempts

   b. Data Processing:
      - Parses voice usage data with specialized format handling
      - Converts voice minutes to standardized units
      - Applies rate calculations and billing logic
      - Handles timezone conversions for usage timestamps

   c. Staging Population:
      - Maps voice data to TelegenceDeviceUsageMubuStaging schema
      - Implements data deduplication using composite keys
      - Applies business rules for voice usage validation
      - Handles null values and missing data scenarios

6. Data Files Processing:
   // GetLatestMubuUsageFileList() Internal Process:
   a. File Discovery:
      - Scans data usage directory with recursive subdirectory support
      - Applies file pattern matching for data usage files
      - Implements file age filtering based on business requirements
      - Handles symbolic links and file system edge cases

   b. Metadata Extraction:
      - Extracts billing period information from file names
      - Validates file naming convention compliance
      - Determines file processing priority based on metadata
      - Logs file discovery metrics for monitoring

   // GetLatestMubuUsage() Internal Process:
   a. Download Management:
      - Implements parallel download capability for multiple files
      - Manages bandwidth throttling to prevent network saturation
      - Handles partial downloads with resume functionality
      - Monitors download progress with real-time status updates

   b. Content Processing:
      - Parses data usage records with format-specific handlers
      - Implements data aggregation for duplicate usage records
      - Applies data quality rules and outlier detection
      - Handles compressed file formats and archives

   c. Database Operations:
      - Performs bulk insert operations with optimized batch sizing
      - Implements constraint handling for foreign key relationships
      - Manages transaction boundaries for data consistency
      - Handles concurrent access scenarios with appropriate locking

7. Failure Tracking and Recovery:
   - Records download failures in TelegenceSFTPFileDownloadStatus
   - Implements failure categorization for root cause analysis
   - Tracks retry attempts with exponential backoff scheduling
   - Provides failure notification system for operational alerts

8. Queue Management Operations:
   // SendMessageToQueueNextDownloadAsync() Process:
   - Constructs SQS messages with processing continuation parameters
   - Implements message deduplication using content-based hashing
   - Sets message visibility timeout based on processing estimates
   - Handles queue capacity limits and throttling scenarios

   // SendMessageToQueueDownloadAgainAsync() Process:
   - Creates retry messages with failure context information
   - Implements delay scheduling for retry attempts
   - Tracks retry chain length to prevent infinite loops
   - Provides escalation mechanism for persistent failures

   // SendProcessMessageToQueueAsync() Process:
   - Generates processing trigger messages for next pipeline stage
   - Includes comprehensive processing context in message attributes
   - Implements message ordering for sequential processing requirements
   - Handles message size limits with payload compression

9. Notification System:
   // BuildNotifyDownLoadFileBlank() Process:
   - Detects empty or corrupt MUBU files during processing
   - Constructs detailed notification messages with file metadata
   - Implements recipient list management based on service provider
   - Handles email template processing and customization
   - Tracks notification delivery status and failures
```

#### Final Usage Reports Processing:

**Method:** `GetLatestFinalUsage()` - Internal Process:
```csharp
1. Final Usage File Discovery:
   - Scans final usage directory using configured path
   - Applies naming pattern matching for final usage files
   - Implements date range filtering based on billing periods
   - Handles file system permissions and access issues

2. Billing Period Extraction:
   - Parses billing period information from filename using regex
   - Validates billing period format and date ranges
   - Converts extracted dates to appropriate timezone
   - Handles edge cases for month-end and year-end periods

3. File Processing Pipeline:
   - Downloads final usage files with integrity verification
   - Parses final usage data with specialized format handling
   - Applies final usage business rules and validations
   - Handles data corrections and adjustments

4. Staging Operations:
   - Populates TelegenceDeviceFinalUsageStaging with processed data
   - Implements data merge operations for incremental updates
   - Handles duplicate detection and resolution
   - Applies final usage calculation algorithms

5. Data Synchronization:
   // UpdateTelegenceFinalUsageFromStaging() Process:
   - Executes stored procedure for final usage data processing
   - Implements data validation and quality checks
   - Handles billing period closure operations
   - Manages final usage approval workflow
   - Updates device status and billing flags
   - Generates final usage reports and summaries
```

### C. MUBU Data Synchronization Flow

**Method:** `ProcessTelegenceMubuUsageDataSync(KeySysLambdaContext context, int serviceProviderId, int telegenceSyncDataStep)`

**Detailed Step-by-Step Processing:**

#### Step 1: UpdateTelegenceMubuUsageFromStaging
```csharp
Internal Process:
1. Stored Procedure Execution:
   - Calls usp_UpdateTelegenceMubuUsageFromStaging with serviceProviderId
   - Implements transaction management with rollback capability
   - Handles concurrent access with appropriate locking strategies
   - Monitors execution time and performance metrics

2. Data Validation and Processing:
   - Validates staging data integrity and completeness
   - Applies business rules for MUBU usage calculations
   - Handles data conflicts and resolution strategies
   - Updates device usage totals and billing amounts

3. Error Handling and Logging:
   - Captures stored procedure execution results
   - Logs data processing statistics and metrics
   - Handles constraint violations and data integrity issues
   - Implements retry logic for transient database errors

4. Next Step Queuing:
   - Constructs SQS message for Step 2 processing
   - Sets message attributes with processing context
   - Implements delay scheduling for processing coordination
   - Handles queue delivery failures with retry mechanisms
```

#### Step 2: UpdateMobilityMubuUsageFromTelegence
```csharp
Internal Process:
1. Mobility Data Synchronization:
   - Executes usp_UpdateMobilityMubuUsageFromTelegence stored procedure
   - Synchronizes MUBU usage data with mobility billing systems
   - Handles data transformation and mapping operations
   - Implements conflict resolution for overlapping data

2. Cross-System Integration:
   - Updates mobility platform with Telegence usage data
   - Handles API rate limiting and throttling scenarios
   - Implements data format conversion for system compatibility
   - Manages authentication and authorization for external systems

3. Staging Table Management:
   - Executes TruncateTableByTableName() for staging cleanup
   - Implements safe truncation with backup verification
   - Handles foreign key constraints during truncation
   - Monitors table space reclamation and optimization

4. Processing Continuation:
   - Sends Step 3 processing message to SQS queue
   - Includes processing metrics and status information
   - Handles message ordering for sequential processing
   - Implements failure notification for processing interruptions
```

#### Step 3: UpdateLateMubuUsageFromTelegence
```csharp
Internal Process:
1. Late Usage Processing:
   - Identifies and processes late-arriving MUBU usage records
   - Implements retroactive billing adjustments and corrections
   - Handles billing period boundary conditions
   - Manages customer notification requirements for late charges

2. Data Reconciliation:
   - Compares processed usage with expected usage patterns
   - Identifies discrepancies and data quality issues
   - Implements automated correction mechanisms where possible
   - Generates exception reports for manual review

3. Final Cleanup Operations:
   - Performs final staging table truncation
   - Implements audit trail creation for processed data
   - Updates processing status and completion timestamps
   - Handles archival of processed data for compliance requirements
```

### D. Daily Processing Initialization Flow

**Method:** `StartDailyDeviceUsageProcessingAsync(KeySysLambdaContext context, bool fromCloudwatchEvent)`

**Detailed Initialization Process:**

#### 1. System Initialization:
```csharp
// InitializeSync() Internal Process:
1. Staging Table Management:
   - Truncates all Telegence staging tables using parameterized commands
   - Implements table-specific truncation with foreign key handling
   - Verifies truncation completion with row count validation
   - Logs truncation results for audit and monitoring

2. Sync Tracking Initialization:
   - Resets processing status flags in TelegenceProcessingStatus table
   - Initializes processing timestamps and counters
   - Creates new processing batch identifiers for tracking
   - Implements distributed locking for concurrent execution prevention

3. Performance Optimization:
   - Rebuilds indexes on staging tables for optimal performance
   - Updates table statistics for query optimization
   - Implements database maintenance operations
   - Monitors database performance metrics during initialization
```

#### 2. Queue Management:
```csharp
// ClearQueue() Internal Process:
1. Queue Purging Operations:
   - Identifies stale messages in processing queues
   - Implements safe message deletion with backup creation
   - Handles message dependencies and ordering requirements
   - Logs queue clearing operations for audit purposes

2. Queue Health Monitoring:
   - Validates queue accessibility and permissions
   - Monitors queue depth and processing capacity
   - Implements queue capacity planning and scaling
   - Handles queue service interruptions and failover
```

#### 3. Service Provider Discovery:
```csharp
// ServiceProviderCommon.GetNextServiceProviderId() Process:
1. Provider Enumeration:
   - Queries ServiceProvider table for Telegence integration enabled providers
   - Filters by active status and integration type
   - Implements provider prioritization based on processing requirements
   - Handles provider configuration validation

2. Integration Validation:
   - Verifies Telegence integration settings completeness
   - Validates authentication credentials and connectivity
   - Implements health checks for provider endpoints
   - Handles provider-specific configuration requirements
```

#### 4. Provider Processing Loop:

**For Each Service Provider:**
```csharp
// AddProviderToQueueAsync() Detailed Process:
1. Provider Authentication Setup:
   // TelegenceCommon.GetTelegenceAuthenticationInformation() Process:
   - Retrieves provider-specific authentication credentials
   - Validates credential format and expiration dates
   - Implements credential caching for performance optimization
   - Handles credential refresh and rotation scenarios

2. Provider Settings Validation:
   - Loads provider settings using context.SettingsRepo.GetTelegenceDeviceSettings()
   - Validates required settings presence and format
   - Implements setting inheritance and override mechanisms
   - Handles provider-specific configuration variations

3. Billing Period Calculation:
   // BillingPeriodHelper.GetBillingPeriodForServiceProvider() Process:
   - Calculates current billing period based on provider timezone
   - Handles billing period boundary conditions and edge cases
   - Implements timezone conversion and daylight saving adjustments
   - Validates billing period consistency with provider configuration

4. Usage Zeroing Logic Implementation:
   - Compares current date with billing period end date + 1
   - Implements ZeroOutUsage() for billing period transitions
   - Handles premiere report delay period processing
   - Manages usage accumulation reset operations

5. Foundation Account Number (FAN) Processing:
   // GetFoundationAccountList() Process:
   - Retrieves active FAN list for service provider
   - Implements FAN filtering based on inclusion/exclusion rules
   - Validates FAN format and accessibility
   - Handles FAN hierarchy and relationship management

6. SFTP Connection Establishment:
   - Creates persistent SFTP connection for file operations
   - Implements connection pooling and reuse strategies
   - Handles authentication and security requirements
   - Monitors connection health and performance

7. FAN-Specific Processing Loop:
   For each FAN:
   a. Latest Usage File Discovery:
      // GetLatestUsageFile() Process:
      - Scans FAN-specific directories for latest usage files
      - Implements file pattern matching and date filtering
      - Handles file naming convention variations
      - Validates file accessibility and permissions

   b. Usage Recency Validation:
      // HasRecentUsage() Process:
      - Compares file modification timestamp with current time
      - Applies configurable recency threshold for validation
      - Handles timezone differences and clock skew issues
      - Implements grace period for file processing delays

   c. Queue Record Management:
      // InsertTelegenceUsageQueueRecord() Process:
      - Creates processing queue record with FAN context
      - Implements duplicate detection and prevention
      - Handles queue record prioritization and ordering
      - Manages queue capacity and throttling

   d. Processing Message Generation:
      // SendProcessMessageToQueueAsync() Process:
      - Constructs SQS message with FAN processing parameters
      - Implements message deduplication and ordering
      - Handles message size optimization and compression
      - Manages message delivery confirmation and retry

8. MUBU Processing Coordination:
   a. MUBU Voice File Validation:
      // GetLatestMubuVoiceFile() Process:
      - Scans MUBU voice directory for recent files
      - Implements file age validation and filtering
      - Handles voice file format variations
      - Validates voice file completeness and integrity

   b. MUBU Usage File Validation:
      // GetLatestMubuUsageFile() Process:
      - Scans MUBU usage directory for current files
      - Implements usage file pattern matching
      - Handles compressed and archived file formats
      - Validates usage file data consistency

   c. Stale File Notification:
      // SendEmailAsync() Process:
      - Identifies files exceeding staleness threshold
      - Constructs notification email with file details
      - Implements recipient list management and customization
      - Handles email delivery tracking and failure handling

   d. MUBU Processing Queue Management:
      - Queues MUBU processing when files are current
      - Implements MUBU processing prioritization
      - Handles MUBU processing dependencies and ordering
      - Manages MUBU processing capacity and throttling

9. Final Usage Processing:
   a. Final Usage File Discovery:
      // GetLatestFinalUsageFile() Process:
      - Scans final usage directory for billing period files
      - Implements billing period extraction from filenames
      - Handles final usage file format validation
      - Validates final usage data completeness

   b. Billing Period Validation:
      // HasOpenBillingPeriod() Process:
      - Checks billing period status in database
      - Validates billing period closure requirements
      - Handles billing period transition scenarios
      - Implements billing period conflict resolution

   c. Final Usage Queue Management:
      - Queues final usage processing for open periods
      - Implements final usage processing prioritization
      - Handles final usage dependencies and prerequisites
      - Manages final usage processing scheduling

10. Kafka Processing Integration:
    - Validates Kafka configuration settings presence
    - Implements Kafka producer setup and configuration
    - Handles Kafka topic management and partitioning
    - Executes Kafka usage update operations with error handling
    - Manages Kafka offset tracking and acknowledgment
    - Implements Kafka retry logic and dead letter handling
```

## Supporting Class Method Details

### AwsFunctionBase.cs Methods

#### Logging and Context Management:
```csharp
// LogInfo() Internal Process:
1. Caller Information Extraction:
   - Uses CallerFilePath, CallerLineNumber, CallerMemberName attributes
   - Extracts source file name and line number for debugging
   - Implements stack trace analysis for call hierarchy
   - Handles anonymous method and lambda expression contexts

2. Log Message Formatting:
   - Applies StringHelper.FormatLogStringObject() for consistent formatting
   - Implements structured logging with key-value pairs
   - Handles object serialization for complex data types
   - Manages log message size limits and truncation

3. Context-Aware Logging:
   - Includes Lambda execution context information
   - Adds correlation IDs for distributed tracing
   - Implements log level filtering based on configuration
   - Handles sensitive data masking and redaction

// BaseFunctionHandler() Internal Process:
1. Context Initialization:
   - Creates new KeySysLambdaContext with Lambda context
   - Initializes logging infrastructure with context binding
   - Sets up environment variable access and caching
   - Implements OU-specific logic initialization when required

2. Database Connection Setup:
   - Establishes central database connection with retry logic
   - Configures connection pooling and timeout settings
   - Implements connection health monitoring
   - Handles connection string decryption and validation

3. Service Registration:
   - Registers Base64 decoding service for encrypted configuration
   - Sets up settings repository with database access
   - Initializes environment repository for configuration access
   - Implements dependency injection container setup

// CleanUp() Internal Process:
1. Resource Disposal:
   - Disposes database connections and command objects
   - Releases memory allocated for large data structures
   - Implements garbage collection hints for memory optimization
   - Handles unmanaged resource cleanup

2. Context State Management:
   - Clears cached configuration values
   - Resets processing state flags and counters
   - Implements context state persistence for debugging
   - Handles context cleanup error scenarios
```

#### Database Operations:
```csharp
// GetCustomerName() Internal Process:
1. Query Execution:
   - Executes parameterized query against RevCustomer table
   - Implements SQL injection prevention with parameter binding
   - Handles database connection management and disposal
   - Implements query timeout and cancellation support

2. Data Retrieval and Processing:
   - Processes SqlDataReader with type-safe value extraction
   - Implements null value handling and default value assignment
   - Handles data type conversion and validation
   - Manages memory usage during data retrieval

3. Error Handling:
   - Implements comprehensive exception handling for SQL operations
   - Handles connection failures with retry logic
   - Logs detailed error information for troubleshooting
   - Implements fallback mechanisms for data retrieval failures

// SqlBulkCopy() Internal Process:
1. Connection Management:
   - Opens SQL connection with optimized settings
   - Implements connection reuse and pooling strategies
   - Handles connection timeout and retry scenarios
   - Manages transaction boundaries for bulk operations

2. Bulk Copy Configuration:
   - Sets destination table name with schema validation
   - Configures bulk copy timeout and batch size for performance
   - Implements column mapping for schema compatibility
   - Handles data type conversion and validation

3. Data Transfer Execution:
   - Executes bulk copy operation with progress monitoring
   - Implements memory optimization for large datasets
   - Handles constraint violations and data integrity issues
   - Manages bulk copy performance tuning and optimization

4. Error Recovery:
   - Implements comprehensive error handling for bulk operations
   - Handles partial failure scenarios with rollback capability
   - Logs detailed error information for troubleshooting
   - Implements retry logic for transient failures
```

#### AWS Operations:
```csharp
// AwsCredentials() Internal Process:
1. Credential Retrieval:
   - Extracts AWS access key from general provider settings
   - Retrieves encrypted secret access key from configuration
   - Implements credential validation and format checking
   - Handles credential caching for performance optimization

2. Decryption Operations:
   - Uses Base64Service for secret key decryption
   - Implements secure memory handling for decrypted credentials
   - Handles decryption errors and fallback mechanisms
   - Manages credential lifecycle and rotation

3. AWS Credential Object Creation:
   - Creates BasicAWSCredentials object with decrypted values
   - Implements credential validation and testing
   - Handles AWS credential format requirements
   - Manages credential object lifecycle and disposal
```

### BillingPeriodHelper.cs Methods

#### Billing Period Operations:
```csharp
// GetBillingPeriodForServiceProvider() Internal Process:
1. Provider Settings Retrieval:
   - Loads service provider specific billing configuration
   - Retrieves billing day of month and timezone settings
   - Implements setting validation and default value assignment
   - Handles provider-specific billing calendar requirements

2. Timezone Processing:
   - Converts UTC timestamps to provider timezone
   - Handles daylight saving time transitions
   - Implements timezone database updates and maintenance
   - Manages timezone conversion accuracy and precision

3. Billing Period Calculation:
   - Calculates billing period start and end dates
   - Handles month boundary conditions and leap years
   - Implements business day adjustments for billing periods
   - Manages billing period overlap and gap prevention

4. Validation and Consistency:
   - Validates calculated billing periods for logical consistency
   - Implements billing period boundary validation
   - Handles edge cases for month-end and year-end periods
   - Manages billing period audit trail and history
```

### ServiceProviderCommon.cs Methods

#### Service Provider Operations:
```csharp
// GetNextServiceProviderId() Internal Process:
1. Database Query Execution:
   - Queries ServiceProvider table with integration type filter
   - Implements active status filtering and validation
   - Handles provider prioritization and ordering logic
   - Manages query performance and optimization

2. Integration Type Validation:
   - Validates integration type against supported values
   - Implements integration capability checking
   - Handles integration version compatibility
   - Manages integration configuration validation

3. Provider Selection Logic:
   - Implements round-robin or priority-based selection
   - Handles provider availability and health checking
   - Manages provider load balancing and distribution
   - Implements provider failover and redundancy

4. Result Processing:
   - Returns next available provider ID with validation
   - Implements provider ID caching for performance
   - Handles provider selection error scenarios
   - Manages provider selection audit and logging
```

### SettingsRepository.cs Methods

#### Settings Retrieval Operations:
```csharp
// GetTelegenceDeviceSettings() Internal Process:
1. Database Connection and Query:
   - Opens SQL connection to central database
   - Executes parameterized query against ServiceProviderSetting table
   - Filters by ServiceProviderId and active status
   - Implements query optimization and performance tuning

2. Settings Parsing and Mapping:
   - Iterates through SqlDataReader results
   - Maps database columns to TelegenceProviderSettings properties
   - Implements type-safe value conversion and validation
   - Handles null values and default setting assignment

3. Configuration Validation:
   - Validates required settings presence and format
   - Implements setting value range and format checking
   - Handles setting dependencies and relationships
   - Manages setting inheritance and override logic

4. Object Construction:
   - Constructs TelegenceProviderSettings object with validated values
   - Implements setting object caching for performance
   - Handles setting object lifecycle and disposal
   - Manages setting change notification and updates

// GetOptimizationSettings() Internal Process:
1. Stored Procedure Execution:
   - Calls stored procedure with tenant ID parameter
   - Implements SQL retry policy with exponential backoff
   - Handles stored procedure timeout and cancellation
   - Manages stored procedure parameter validation

2. Settings Collection Processing:
   - Processes list of optimization settings from database
   - Implements setting key-value pair mapping
   - Handles setting override logic and inheritance
   - Manages setting validation and consistency checking

3. Timezone Configuration:
   - Attempts Linux timezone ID resolution first
   - Falls back to Windows timezone ID if Linux fails
   - Implements timezone validation and error handling
   - Manages timezone database updates and maintenance

4. Settings Object Assembly:
   - Maps individual settings to OptimizationSettings properties
   - Implements setting type conversion and validation
   - Handles setting default value assignment
   - Manages settings object lifecycle and caching
```

### TelegenceCommon.cs Methods

#### Authentication and Account Management:
```csharp
// GetTelegenceAuthenticationInformation() Internal Process:
1. Provider Authentication Lookup:
   - Queries IntegrationAuthentication table by service provider
   - Validates authentication record existence and status
   - Implements authentication caching for performance
   - Handles authentication credential rotation and updates

2. Credential Processing:
   - Decrypts stored authentication credentials
   - Validates credential format and expiration
   - Implements credential testing and validation
   - Manages credential security and access control

3. Authentication Object Construction:
   - Creates TelegenceAuthentication object with validated credentials
   - Implements authentication object caching and lifecycle
   - Handles authentication failure scenarios and fallbacks
   - Manages authentication audit logging and monitoring

// GetTelegenceDevicesAsync() Internal Process:
1. HTTP Client Configuration:
   - Configures HTTP client with timeout and retry settings
   - Implements authentication header setup
   - Handles proxy configuration and routing
   - Manages HTTP client lifecycle and disposal

2. API Request Construction:
   - Builds paginated API request with query parameters
   - Implements request serialization and encoding
   - Handles request size limits and optimization
   - Manages request correlation and tracking

3. Response Processing:
   - Processes paginated API responses with continuation tokens
   - Implements response deserialization and validation
   - Handles partial response scenarios and errors
   - Manages response data transformation and mapping

4. Device Data Processing:
   - Extracts device information from API responses
   - Implements device data validation and cleansing
   - Handles device status mapping and transformation
   - Manages device data caching and persistence
```

## Data Flow Summary

### 1. Initialization Phase
```csharp
Process Details:
- Lambda receives SQS event or CloudWatch trigger with context validation
- Environment variables loaded with decryption and validation
- Database connections established with retry logic and health checking
- Service dependencies initialized with error handling and fallback
- Processing state initialized with distributed locking and coordination
```

### 2. Provider Discovery Phase
```csharp
Process Details:
- Service providers identified with integration type filtering
- Provider-specific settings retrieved with validation and caching
- Authentication credentials loaded with decryption and testing
- Billing periods calculated with timezone and business rule handling
- FAN lists obtained with filtering and validation logic
```

### 3. File Discovery Phase
```csharp
Process Details:
- SFTP connections established with authentication and health checking
- File directories scanned with pattern matching and filtering
- File timestamps validated with recency and business rule checking
- File accessibility tested with permissions and integrity validation
- File processing priority determined with business logic and dependencies
```

### 4. File Processing Phase
```csharp
Process Details:
- Files downloaded with streaming, resume capability, and integrity checking
- Data extracted with format-specific parsing and validation
- Business rules applied with error handling and data quality checking
- Staging tables populated with bulk operations and transaction management
- Processing progress tracked with checkpoints and status updates
```

### 5. Data Synchronization Phase
```csharp
Process Details:
- Staged data processed through stored procedures with transaction management
- Main device tables updated with conflict resolution and data validation
- Late records synchronized with retroactive processing and billing adjustments
- Mobility data integrated with cross-system coordination and error handling
- Data consistency validated with reconciliation and audit procedures
```

### 6. Cleanup and Notification Phase
```csharp
Process Details:
- Old files cleaned up with retention policy enforcement and audit logging
- Processing queues managed with message lifecycle and error handling
- Notification emails sent with template processing and delivery tracking
- Processing status updated with completion timestamps and metrics
- Lambda execution completed with resource cleanup and performance logging
```

## Error Handling and Retry Logic

### SQL Retry Policy
```csharp
Implementation Details:
- Transient SQL exceptions identified using error code analysis
- Retry attempts limited to maximum 3 with exponential backoff
- Retry delays calculated using base delay * Math.Pow(2, attemptNumber)
- Timeout exceptions included in retry scope with separate handling
- Retry state persisted for debugging and monitoring purposes
```

### File Download Failures
```csharp
Implementation Details:
- Failed downloads tracked in TelegenceSFTPFileDownloadStatus with detailed metadata
- Retry messages constructed with failure context and processing parameters
- Progressive processing implemented with continuation tokens and state management
- Failure categorization applied for root cause analysis and resolution
- Escalation procedures triggered for repeated failures exceeding thresholds
```

### Email Notifications
```csharp
Implementation Details:
- Stale file notifications generated with configurable threshold checking
- Blank file notifications sent for empty MUBU files with metadata analysis
- Error notifications dispatched for processing failures with detailed context
- Notification templates processed with dynamic content and personalization
- Delivery tracking implemented with retry logic and failure handling
```

## Configuration Dependencies

### Environment Variables
```csharp
Required Configuration:
- TelegenceDeviceUsageQueueURL: SQS queue for device usage processing messages
- TelegenceDeviceNotificationQueueURL: SQS queue for notification messages
- DeviceCleanupMaxRetries: Maximum retry attempts for cleanup operations
- DaysToKeep: File retention period for cleanup operations
- FtpReportNotificationThresholdDays: Threshold for stale file notifications
- CheckFilesMissedThresholdDays: Threshold for missed file detection
- LimitAmountFilePerRunTimes: Batch size limit for file processing
- PremiereReportDelayDays: Delay period for premiere report processing
- IsPremiereReportDelaySimulator: Flag for premiere report delay simulation
- DayEndBillingSimulator: Flag for billing day simulation
- MUBURowsCountLimit: Row count limit for MUBU processing batches
```

### Database Settings
```csharp
Required Configuration:
- Telegence FTP server credentials with encryption and secure storage
- AWS access keys with role-based permissions and rotation support
- SES credentials for email notification delivery
- Jasper database connection strings with connection pooling
- Kafka configuration parameters for message streaming
- Email notification settings with template and recipient management
```

This comprehensive flow documentation covers all major methods and their detailed internal implementations within the Telegence device usage processing system, providing the detailed process descriptions requested rather than simple method call references.