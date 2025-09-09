using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Services;
using Altaworx.AWS.Core.Services.Optimization;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SimpleEmail.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
//to use SQL timeout constant
using Amop.Core.Constants;
using Amop.Core.Logger;
using Amop.Core.Models;
using Amop.Core.Models.Integration;
using Amop.Core.Models.Settings;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Integration;
using Amop.Core.Repositories.Optimization;
using Amop.Core.Services.Base64Service;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.SqlServer.Storage.Internal;
using Microsoft.Extensions.DependencyInjection;
using MimeKit;
using Polly;
using Polly.Retry;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using AMOPConstants = Amop.Core.Constants.SQLConstant;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxJasperGetDevicesCleanup
{
    public class Function : AwsFunctionBase
    {
        private string DeviceSyncSummaryLogS3BucketName;
        private bool DeviceSyncSummaryLogEnable;
        private const int SQL_TRANSIENT_RETRY_BASE_SECONDS = 4;
        private const int SQL_TRANSIENT_RETRY_MAX_COUNT = 3;
        private const int SQL_ERROR_NUMBER_DB_TIMEOUT = -2;
        private string DeviceNotificationQueueURL = Environment.GetEnvironmentVariable("DeviceNotificationQueueURL");
        private string CarrierOptimizationQueueURL = Environment.GetEnvironmentVariable("CarrierOptimizationQueueURL");
        private readonly EnvironmentRepository environmentRepo = new EnvironmentRepository();
        private Altaworx.AWS.Core.IS3Wrapper s3Wrapper;
        private DailySyncAmopApiTrigger dailySyncAmopApiTrigger = new DailySyncAmopApiTrigger();
        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="sqsEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            try
            {
                keysysContext = BaseFunctionHandler(context);
                InitializeServices(keysysContext);

                if (string.IsNullOrEmpty(DeviceNotificationQueueURL))
                {
                    DeviceNotificationQueueURL = context.ClientContext.Environment["DeviceNotificationQueueURL"];
                    CarrierOptimizationQueueURL = context.ClientContext.Environment["CarrierOptimizationQueueURL"];
                }

                await ProcessEventAsync(keysysContext, sqsEvent);
            }
            catch (Exception ex)
            {
                // use lambda logger in case exception occurs in creating original context object
                context.Logger.LogLine($"EXCEPTION: {ex.Message} {ex.StackTrace}");
            }
            IntegrationType IntegrationId = (IntegrationType)Convert.ToInt32(sqsEvent.Records[0].MessageAttributes["IntegrationType"].StringValue);
            var keyName = "";
            switch (IntegrationId)
            {
                case IntegrationType.ThingSpace:
                    keyName = "thingspace_devices";
                    break;
                case IntegrationType.Telegence:
                    keyName = "telegence_devices";
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
                    LogInfo(keysysContext, LogTypeConstant.Exception, LogCommonStrings.INTEGRATION_TYPE_IS_UNSUPPORTED);
                    throw new Exception(LogCommonStrings.INTEGRATION_TYPE_IS_UNSUPPORTED);
            }
            // get tenant id passing 1 if serviceProviderid is null - it will get tenant id based on conenction string
            var serviceProviderId = 1;
            if (sqsEvent.Records[0].MessageAttributes.ContainsKey("ServiceProviderId"))
            {
                serviceProviderId = Convert.ToInt32(sqsEvent.Records[0].MessageAttributes["ServiceProviderId"].StringValue);
            }
            var serviceProvider = ServiceProviderCommon.GetServiceProvider(keysysContext.CentralDbConnectionString, serviceProviderId);
            dailySyncAmopApiTrigger.SendNotificationToAmop20(keysysContext, context, keyName, serviceProvider.TenantId, null);
            keysysContext?.CleanUp();
        }

        private void InitializeServices(KeySysLambdaContext lambdaContext)
        {
            DeviceSyncSummaryLogS3BucketName = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo, EnvironmentVariableKeyConstants.DEVICE_SYNC_SUMMARY_LOG_S3_BUCKET_NAME);
            DeviceSyncSummaryLogEnable = GetBooleanValueFromEnvironmentVariable(lambdaContext, environmentRepo, EnvironmentVariableKeyConstants.DEVICE_SYNC_SUMMARY_LOG_ENABLE);
            if (DeviceSyncSummaryLogEnable)
            {
                var base64Service = new Base64Service();
                var settingsRepository = new SettingsRepository(lambdaContext.logger, lambdaContext.CentralDbConnectionString, base64Service);
                var generalProviderSettings = settingsRepository.GetGeneralProviderSettings();
                s3Wrapper = new Altaworx.AWS.Core.S3Wrapper(generalProviderSettings.AwsCredentials, DeviceSyncSummaryLogS3BucketName);
            }
        }

        private async Task ProcessEventAsync(KeySysLambdaContext context, SQSEvent sqsEvent)
        {
            LogInfo(context, "SUB", "ProcessEventAsync");
            if (sqsEvent.Records.Count > 0)
            {
                if (sqsEvent.Records.Count == 1)
                {
                    await ProcessEventRecordAsync(context, sqsEvent.Records[0]);
                }
                else
                {
                    LogInfo(context, "EXCEPTION", $"Expected a single message, received {sqsEvent.Records.Count}");
                }
            }
        }

        private GetDevicesCleanupSqsValues GetMessageQueueValues(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            return new GetDevicesCleanupSqsValues(context, message);
        }

        private async Task ProcessEventRecordAsync(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            LogInfo(context, LogTypeConstant.Sub, "");

            var sqsValues = GetMessageQueueValues(context, message);
            var hasSendMessageToCarrierOptimization = false;
            try
            {
                if (!sqsValues.IntegrationTypeReceived)
                {
                    LogInfo(context, CommonConstants.EXCEPTION, LogCommonStrings.INTEGRATION_TYPE_IS_NOT_BEING_SPECIFIED);
                    return;
                }

                var integrationTypeRepository = new IntegrationTypeRepository(context.CentralDbConnectionString);
                var integrationTypes = integrationTypeRepository.GetIntegrationTypes();

                int remainingRowsToProcess = CountRowsToProcess(context, sqsValues.IntegrationType);
                LogInfo(context, CommonConstants.INFO, $"{LogCommonStrings.CURRENT_REMAINING_ROWS_TO_PROCESS}={remainingRowsToProcess}");
                if (IsTooManyRetries(remainingRowsToProcess, sqsValues))
                {
                    LogInfo(context, CommonConstants.EXCEPTION, LogCommonStrings.TOO_MANY_RETRIES);
                    ClearRowsToProcess(context, sqsValues.IntegrationType);
                    remainingRowsToProcess = 0;
                }

                if (remainingRowsToProcess == 0)
                {
                    var policyFactory = new Amop.Core.Resilience.PolicyFactory(context.logger);
                    var sqlTransientRetryPolicy = policyFactory.GetSqlRetryPolicy(SQL_TRANSIENT_RETRY_MAX_COUNT);
                    // Sync to devices
                    SyncDeviceTables(context, sqsValues, sqlTransientRetryPolicy);

                    // Update Comm plans
                    await UpdateCommPlans(context, sqsValues);

                    // Get Summary Values
                    var summary = sqlTransientRetryPolicy
                        .ExecuteAndCapture(() => GetSummaryValues(context, sqsValues.IntegrationType, sqsValues.ServiceProviderId)).Result;

                    // Should this carrier go to snowflake historian?
                    bool shouldGoToHistorian = !IntegrationTypeIsMobility(sqsValues.IntegrationType) && !string.IsNullOrWhiteSpace(sqsValues.SnowflakeS3BucketName) && !string.IsNullOrWhiteSpace(sqsValues.SnowflakeS3BucketPath);

                    // Send Summary Email with charge list file
                    try
                    {
                        await SendEmailSummaryAsync(context, sqsValues.IntegrationType, summary, shouldGoToHistorian, integrationTypes);
                    }
                    catch (Exception ex)
                    {
                        LogInfo(context, "EXCEPTION", ex.Message + " " + ex.StackTrace);
                    }

                    // Check to see if snowflake S3 is configured
                    if (shouldGoToHistorian)
                    {
                        SendDeviceHistoryToSnowflake(context, sqsValues);
                    }

                    // Check to see if it's time to queue carrier optimization
                    if ((sqsValues.IntegrationType == IntegrationType.Jasper ||
                        sqsValues.IntegrationType == IntegrationType.POD19 ||
                        sqsValues.IntegrationType == IntegrationType.TMobileJasper ||
                        sqsValues.IntegrationType == IntegrationType.Rogers) &&
                        sqsValues.ShouldQueueCarrierOptimization)
                    {
                        await SendCarrierOptimizationMessageToQueue(context, sqsValues.ServiceProviderId, sqsValues.OptimizationSessionId);
                        hasSendMessageToCarrierOptimization = true;
                    }

                    // Check devices for billing period discrepancy and send email to notify
                    if (sqsValues.IntegrationType == IntegrationType.ThingSpace)
                    {
                        var deviceDiscrepancyService = new ThingSpaceDiscrepancyService();
                        await deviceDiscrepancyService.CheckDevicesForBillingPeriodDiscrepancy(context, sqsValues);
                    }
                }
                else
                {
                    // Not ready, send back to queue
                    await SendNotificationMessageToQueue(context, sqsValues, sqsValues.RetryCount + 1, remainingRowsToProcess, sqsValues.IntegrationType);
                }
            }
            catch (Exception ex)
            {
                if (sqsValues.ShouldQueueCarrierOptimization && !hasSendMessageToCarrierOptimization)
                {
                    await OptimizationUsageSyncErrorHandler.ProcessStopCarrierOptimization(context, sqsValues.ServiceProviderId, sqsValues.OptimizationSessionId, ex.Message);
                }
                LogInfo(context, CommonConstants.EXCEPTION, $"{LogCommonStrings.ERROR_WHILE_SYNCING_DEVICE_CLEANUP} - {ex.Message} - {ex.StackTrace}");
            }
        }

        private bool IsTooManyRetries(int remainingRowsToProcess, GetDevicesCleanupSqsValues sqsValues)
        {
            switch (sqsValues.IntegrationType)
            {
                case IntegrationType.eBonding:
                    if (sqsValues.RemainingRowsToProcess == null && remainingRowsToProcess != 0 && (sqsValues.RetryCount + 1) > sqsValues.MaxRetries)
                    {
                        // no previous remaining rows to process and exceeded retry count
                        return true;
                    }
                    else if (sqsValues.RemainingRowsToProcess != null && sqsValues.RemainingRowsToProcess.Value == remainingRowsToProcess)
                    {
                        // previous and current remaining rows to process has not changed
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                default:
                    return remainingRowsToProcess != 0 && (sqsValues.RetryCount + 1) > sqsValues.MaxRetries;
            }
        }

        public void SendDeviceHistoryToSnowflake(KeySysLambdaContext context, GetDevicesCleanupSqsValues sqsValues)
        {
            string textToWrite = GetDeviceHistoryString(context, sqsValues);
            byte[] fileBytes = Encoding.UTF8.GetBytes(textToWrite);
            S3Wrapper s3Wrapper = new S3Wrapper(base.AwsCredentials(context), sqsValues.SnowflakeS3BucketName);
            string fileName = $"{sqsValues.SnowflakeS3BucketPath}/DeviceHistory_{sqsValues.ServiceProviderId}_{DateTime.UtcNow.ToString("yyyyMMddhhmmss")}.csv";
            s3Wrapper.UploadAwsFile(fileBytes, fileName);
        }

        private string GetDeviceHistoryString(KeySysLambdaContext context, GetDevicesCleanupSqsValues sqsValues)
        {
            var sqlText = @"SELECT d.[id]
      ,d.[ServiceProviderId]
      ,d.[ICCID]
      ,d.[IMSI]
      ,d.[MSISDN]
      ,d.[IMEI]
      ,d.[DeviceStatusId]
      ,d.[Status]
      ,d.[CarrierRatePlanId]
      ,d.[RatePlan]
      ,d.[CommunicationPlan]
      ,d.[LastUsageDate]
      ,d.[APN]
      ,d.[Package]
      ,d.[CtdDataUsage]
      ,d.[CtdSMSUsage]
      ,d.[CtdVoiceUsage]
      ,d.[CtdSessionCount]
      ,d.[OverageLimitReached]
      ,d.[OverageLimitOverride]
      ,d.[CreatedBy]
      ,d.[CreatedDate]
      ,d.[ModifiedBy]
      ,d.[ModifiedDate]
      ,d.[LastActivatedDate]
      ,d.[DeletedBy]
      ,d.[DeletedDate]
      ,d.[IsActive]
      ,d.[IsDeleted]
      ,dt.[AccountNumber]
      ,d.[ProviderDateAdded]
      ,d.[ProviderDateActivated]
      ,d.[OldDeviceStatusId]
      ,d.[OldCtdDataUsage]
      ,d.[CostCenter]
      ,d.[Username]
      ,dt.[AccountNumberIntegrationAuthenticationId]
      ,[BillingPeriodId]
      ,dt.[SiteId]
FROM [dbo].[Device] d 
INNER JOIN [dbo].[ServiceProvider] sp ON d.ServiceProviderId = sp.id
INNER JOIN [dbo].[Device_Tenant] dt ON d.id = dt.DeviceId AND dt.TenantId = sp.TenantId
WHERE d.ServiceProviderId = @ServiceProviderId";
            StringBuilder sb = new StringBuilder();
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand(sqlText, con)
                {
                    CommandType = CommandType.Text
                })
                {
                    con.Open();
                    cmd.Parameters.AddWithValue("@ServiceProviderId", sqsValues.ServiceProviderId);
                    using (var rdr = cmd.ExecuteReader())
                    {
                        if (rdr.HasRows)
                        {
                            while (rdr.Read())
                            {
                                sb.AppendLine(DeviceHistoryFileLineFromReader(rdr));
                            }
                        }
                    }
                }

                con.Close();
            }

            return sb.ToString();
        }

        private string DeviceHistoryFileLineFromReader(SqlDataReader rdr)
        {
            // init with two blank columns
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < rdr.FieldCount; i++)
            {
                if (i > 0)
                {
                    sb.Append(",");
                }

                if (!rdr.IsDBNull(i))
                {
                    var fieldType = rdr.GetFieldType(i);
                    // check for date field
                    if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                    {
                        // convert date to format recognized by snowflake
                        var tempDate = rdr.GetDateTime(i);
                        sb.Append(tempDate.ToString("yyyy-MM-ddThh:mm:ssZ"));
                    }
                    else
                    {
                        sb.Append(rdr[i]);
                    }
                }
            }

            return sb.ToString();
        }

        private async Task UpdateCommPlans(KeySysLambdaContext context, GetDevicesCleanupSqsValues sqsValues)
        {
            LogInfo(context, "SUB", "UpdateCommPlansAsync");
            var newPlans = new List<string>();

            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                con.Open();

                using (var cmd = new SqlCommand(
                    @"SELECT DISTINCT d.CommunicationPlan
                      FROM [dbo].[Device] d
                      LEFT JOIN [dbo].[JasperCommunicationPlan] jcp ON jcp.CommunicationPlanName = d.CommunicationPlan
                      WHERE d.ServiceProviderId = @ServiceProviderId
                      AND jcp.CommunicationPlanName IS NULL
                      AND d.CommunicationPlan IS NOT NULL", con))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = 60;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", sqsValues.ServiceProviderId);
                    using (var rdr = cmd.ExecuteReader())
                    {
                        if (rdr.HasRows)
                        {
                            while (rdr.Read())
                            {

                                newPlans.Add(rdr[0].ToString());
                            }
                        }
                    }
                }

                foreach (var plan in newPlans)
                {
                    using (var cmd = new SqlCommand(
                        @"INSERT INTO [dbo].[JasperCommunicationPlan] (CommunicationPlanName, CreatedBy, CreatedDate, IsActive, IsDeleted, ServiceProviderId)
                          VALUES (@CommPlan, 'JasperDeviceSync', GETUTCDATE(), 1, 0, @ServiceProviderId)", con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandTimeout = 60;
                        cmd.Parameters.AddWithValue("@CommPlan", plan);
                        cmd.Parameters.AddWithValue("@ServiceProviderId", sqsValues.ServiceProviderId);
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            if (newPlans.Count > 0)
            {
                try
                {
                    await SendEmailAsync(context, "New Communication Plans Added", BuildNewCommPlanEmailBody(context, newPlans));
                }
                catch (Exception ex)
                {
                    LogInfo(context, "EXCEPTION", ex.Message + " " + ex.StackTrace);
                }
            }
        }

        private BodyBuilder BuildNewCommPlanEmailBody(KeySysLambdaContext context, IReadOnlyCollection<string> newPlans)
        {
            LogInfo(context, "SUB", "BuildNewCommPlanEmailBody()");
            return new BodyBuilder
            {
                HtmlBody = @$"<html>
                    <h2>New Communication Plans Added</h2>
                    <p>
                        These communication plans need to be associated to rate plans in order to optimize or deactivate devices.
                        See bottom of Carrier Rate Plans page to edit Communication Plans.
                    </p>
                    <h3>Communication Plans Added:</h3>
                    <ul>
                        {string.Join("", newPlans.Select(plan => $"<li>{plan}</li>"))}
                    </ul>
                </html>",
                TextBody = @$"
New Communication Plans Added
                    
These communication plans need to be associated to rate plans in order to optimize or deactivate devices.
See bottom of Carrier Rate Plans page to edit Communication Plans.

Communication Plans Added:

{string.Join(Environment.NewLine, newPlans)}"
            };
        }

        private async Task SendCarrierOptimizationMessageToQueue(KeySysLambdaContext context, int serviceProviderId, long optimizationSessionId)
        {
            LogInfo(context, "SUB", $"SendCarrierOptimizationMessageToQueue({serviceProviderId})");

            // get tenant and billing period
            var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, serviceProviderId);

            LogInfo(context, "INFO", $"Finding billing period for Service Provider {serviceProviderId}. {DateTime.Now.Month}/{DateTime.Now.Year}.");
            var billingPeriod = BillingPeriodHelper.GetBillingPeriodForServiceProvider(context.CentralDbConnectionString, serviceProviderId, DateTime.Now.Year, DateTime.Now.Month, context.OptimizationSettings.BillingTimeZone);

            if (billingPeriod == null || billingPeriod.Id == 0)//billing period not found
            {
                //get the next month billing period in case of new service provider
                LogInfo(context, "INFO", $"Finding billing period for Service Provider {serviceProviderId}. {DateTime.Now.AddMonths(1).Month}/{DateTime.Now.AddMonths(1).Year}.");
                billingPeriod = BillingPeriodHelper.GetBillingPeriodForServiceProvider(context.CentralDbConnectionString, serviceProviderId, DateTime.Now.AddMonths(1).Year, DateTime.Now.AddMonths(1).Month, context.OptimizationSettings.BillingTimeZone);

                // if there are still no billing period found, log out an Exception rather than queue up optimization
                // separated check for clearer logs
                if (billingPeriod.Id == 0)
                {
                    LogInfo(context, "EXCEPTION", $"Cannot Queue Carrier Optimization for Service Provider {serviceProviderId}. No Billing period found.");
                    return;
                }
            }

            // send to queue 
            if (serviceProvider.TenantId != null)
            {
                await SendCarrierOptimizationMessageToQueue(context, serviceProvider.TenantId.Value, billingPeriod, optimizationSessionId);
            }
            else
            {
                LogInfo(context, "EXCEPTION", $"Cannot Queue Carrier Optimization for Service Provider {serviceProviderId}. No Tenant Id configured.");
            }
        }

        private async Task SendCarrierOptimizationMessageToQueue(KeySysLambdaContext context, int tenantId, BillingPeriod billingPeriod, long optimizationSessionId)
        {
            LogInfo(context, "SUB", $"SendCarrierOptimizationMessageToQueue({tenantId},{billingPeriod.ServiceProviderId},{billingPeriod.Id})");
            LogInfo(context, "DEBUG", $"BillPeriodId: {billingPeriod.Id})");
            LogInfo(context, "DEBUG", $"BillYear: {billingPeriod.BillingPeriodYear})");
            LogInfo(context, "DEBUG", $"BillMonth: {billingPeriod.BillingPeriodMonth})");
            LogInfo(context, "DEBUG", $"ServiceProviderId: {billingPeriod.ServiceProviderId})");
            LogInfo(context, "DEBUG", $"TenantId: {tenantId})");
            LogInfo(context, "DEBUG", $"HasSynced: {true})");
            LogInfo(context, "DEBUG", $"OptimizationSessionId: {optimizationSessionId})");

            if (string.IsNullOrWhiteSpace(CarrierOptimizationQueueURL))
            {
                return; // to be able to skip enqueuing messages in a test
            }

            try
            {
                var awsCredentials = context.GeneralProviderSettings.AwsCredentials;
                using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
                {

                    var requestMsgBody = $"Carrier to optimize is for Billing Period {billingPeriod.BillingPeriodYear}/{billingPeriod.BillingPeriodMonth}";
                    var request = new SendMessageRequest
                    {
                        MessageAttributes = new Dictionary<string, MessageAttributeValue>
                        {
                            {
                                "BillYear", new MessageAttributeValue
                                    { DataType = "String", StringValue = billingPeriod.BillingPeriodYear.ToString()}
                            },
                            {
                                "BillMonth", new MessageAttributeValue
                                    { DataType = "String", StringValue = billingPeriod.BillingPeriodMonth.ToString()}
                            },
                            {
                                "ServiceProviderId", new MessageAttributeValue
                                    { DataType = "String", StringValue = billingPeriod.ServiceProviderId.ToString()}
                            },
                            {
                                "TenantId", new MessageAttributeValue
                                    { DataType = "String", StringValue = tenantId.ToString()}
                            },
                            {
                                "BillPeriodId", new MessageAttributeValue
                                    { DataType = "String", StringValue = billingPeriod.Id.ToString()}
                            },
                            {
                                "HasSynced", new MessageAttributeValue
                                    { DataType = "String", StringValue = true.ToString()}
                            }
                        },
                        MessageBody = requestMsgBody,
                        QueueUrl = CarrierOptimizationQueueURL
                    };

                    if (optimizationSessionId > 0)
                    {
                        request.MessageAttributes.Add("OptimizationSessionId", new MessageAttributeValue { DataType = "String", StringValue = optimizationSessionId.ToString() });
                    }

                    var response = await client.SendMessageAsync(request);
                    if ((int)response.HttpStatusCode < 200 || (int)response.HttpStatusCode > 299)
                    {
                        LogInfo(context, "EXCEPTION", $"Error Queuing Optimization: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION", $"Error Queuing Optimization: {ex.Message} {ex.StackTrace}");
            }
        }

        private void ClearRowsToProcess(KeySysLambdaContext context, IntegrationType integrationType)
        {
            LogInfo(context, "SUB", $"ClearRowsToProcess({integrationType})");

            string connectionString = context.GeneralProviderSettings.JasperDbConnectionString;
            string sqlText = "DELETE FROM [dbo].[JasperDeviceUsageICCICDsToProcess]";
            if (integrationType == IntegrationType.ThingSpace)
            {
                connectionString = context.CentralDbConnectionString;
                sqlText = "DELETE FROM [dbo].[ThingSpaceDeviceUsageICCICDsToProcess]";
            }
            else if (integrationType == IntegrationType.Telegence)
            {
                connectionString = context.CentralDbConnectionString;
                sqlText = "DELETE FROM [dbo].[TelegenceDeviceDetailIdsToProcess]";
            }
            else if (integrationType == IntegrationType.eBonding)
            {
                connectionString = context.CentralDbConnectionString;
                sqlText = @"DELETE FROM [dbo].[eBondingDeviceDetailIdsToProcess];
                            DELETE FROM [dbo].[eBondingDeviceUsageIdsToProcess];";
            }
            else if (integrationType == IntegrationType.Pond)
            {
                connectionString = context.CentralDbConnectionString;
                sqlText = @"DELETE FROM [dbo].[PondDeviceStatusICCIDsToProcess]";
            }

            //do loop from proc to get list of items, limit to batch size
            using (var con = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sqlText, con)
                {
                    CommandType = CommandType.Text
                })
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
                // Close destination connection
                con.Close();
            }
        }

        private int CountRowsToProcess(KeySysLambdaContext context, IntegrationType integrationType)
        {
            int rowCount = 0;

            string connectionString = context.GeneralProviderSettings.JasperDbConnectionString;
            string sqlText = "SELECT COUNT(1) AS [RowCount] FROM [dbo].[JasperDeviceUsageICCICDsToProcess]";
            if (integrationType == IntegrationType.ThingSpace)
            {
                connectionString = context.CentralDbConnectionString;
                sqlText = "SELECT COUNT(1) AS [RowCount] FROM [dbo].[ThingSpaceDeviceUsageICCICDsToProcess]";
            }
            else if (integrationType == IntegrationType.Telegence)
            {
                connectionString = context.CentralDbConnectionString;
                sqlText = "SELECT COUNT(1) AS [RowCount] FROM [dbo].[TelegenceDeviceDetailIdsToProcess] where IsDeleted = 0";
            }
            else if (integrationType == IntegrationType.eBonding)
            {
                connectionString = context.CentralDbConnectionString;
                sqlText = @"SELECT SUM(rows) AS [RowCount]
                            FROM (
                                SELECT COUNT(1) AS [rows] 
                                FROM [dbo].[eBondingDeviceDetailIdsToProcess]
                                WHERE RetryCount < 2
                                UNION ALL 
                                SELECT COUNT(1)
                                FROM [dbo].[eBondingDeviceUsageIdsToProcess]
                                WHERE RetryCount < 2
                            ) eBondingDeviceIdsToProcess";
            }
            else if (integrationType == IntegrationType.Teal)
            {
                return rowCount;
            }
            else if (integrationType == IntegrationType.Pond)
            {
                connectionString = context.CentralDbConnectionString;
                sqlText = "SELECT COUNT(1) AS [RowCount] FROM [dbo].[PondDeviceStatusICCIDsToProcess] WHERE [IsDeleted] = 0";
            }

            //do loop from proc to get list of items, limit to batch size
            using (var con = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sqlText, con)
                {
                    CommandType = CommandType.Text
                })
                {
                    con.Open();
                    using (var rdr = cmd.ExecuteReader())
                    {
                        if (rdr.HasRows)
                        {
                            while (rdr.Read())
                            {
                                rowCount = rdr.GetInt32(0);
                            }
                        }
                    }
                }
                // Close destination connection
                con.Close();
            }

            return rowCount;
        }

        private int CountRowsTelegenceDeviceStaging(KeySysLambdaContext context)
        {
            int rowCount = 0;

            string connectionString = context.CentralDbConnectionString;
            string sqlText = "SELECT COUNT(1) AS [RowCount] FROM [dbo].[TelegenceDeviceStaging]";

            //do loop from proc to get list of items, limit to batch size
            using (var con = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sqlText, con)
                {
                    CommandType = CommandType.Text
                })
                {
                    con.Open();
                    using (var rdr = cmd.ExecuteReader())
                    {
                        if (rdr.HasRows)
                        {
                            while (rdr.Read())
                            {
                                rowCount = rdr.GetInt32(0);
                            }
                        }
                    }
                }
                // Close destination connection
                con.Close();
            }

            return rowCount;
        }

        private void SyncDeviceTables(KeySysLambdaContext context, GetDevicesCleanupSqsValues sqsValues, ISyncPolicy sqlTransientRetryPolicy)
        {
            var serviceProviderId = sqsValues.ServiceProviderId;
            var integrationType = sqsValues.IntegrationType;

            switch (integrationType)
            {
                case IntegrationType.ThingSpace:
                    sqlTransientRetryPolicy.Execute(() => SyncThingSpaceDevices(serviceProviderId, context.CentralDbConnectionString, context.logger));
                    if (DeviceSyncSummaryLogEnable)
                    {
                        var thingSpaceFile = GenerateDeviceSyncSummaryLogFile(GetDeviceSyncSummaryLogs(context, serviceProviderId, DeviceSyncSummaryLogForThingSpace(serviceProviderId)));
                        var isThingSpaceUploaded = UploadDeviceSyncSummaryLogToS3(context, thingSpaceFile, "ThingSpace-" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt");
                    }
                    break;
                case IntegrationType.Telegence:
                    // Update Only table TelegenceDeviceStaging has data.
                    var rowCount = CountRowsTelegenceDeviceStaging(context);
                    if (rowCount > 0)
                    {
                        sqlTransientRetryPolicy.Execute(() => UpdateTelegenceDevicesFromStaging(context, serviceProviderId));
                    }
                    sqlTransientRetryPolicy.Execute(() => UpdateTelegenceDeviceDetailsFromStaging(context, serviceProviderId));
                    sqlTransientRetryPolicy.Execute(() => UpdateMobilityFeatureFromStaging(context));
                    sqlTransientRetryPolicy.Execute(() => UpdateTelegenceUsageFromStaging(context, serviceProviderId));
                    sqlTransientRetryPolicy.Execute(() => UpdateTelegenceMubuUsageFromStaging(context, serviceProviderId));
                    sqlTransientRetryPolicy.Execute(() => RunTelegenceDeviceSync(context, serviceProviderId));
                    if (DeviceSyncSummaryLogEnable)
                    {
                        var telegenceFile = GenerateDeviceSyncSummaryLogFile(GetDeviceSyncSummaryLogs(context, serviceProviderId, DeviceSyncSummaryLogForTelegence(serviceProviderId)));
                        var isTelegenceUploaded = UploadDeviceSyncSummaryLogToS3(context, telegenceFile, "Telegence-" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt");
                    }
                    break;
                case IntegrationType.eBonding:
                    sqlTransientRetryPolicy.Execute(() => UpdateEbondingUsage(serviceProviderId, context.CentralDbConnectionString, context.logger));
                    sqlTransientRetryPolicy.Execute(() => SyncEbondingDevices(serviceProviderId, context.CentralDbConnectionString, context.logger));
                    break;
                case IntegrationType.Teal:
                    sqlTransientRetryPolicy.Execute(() => SyncTealDevices(serviceProviderId, context));
                    if (DeviceSyncSummaryLogEnable)
                    {
                        var tealFile = GenerateDeviceSyncSummaryLogFile(GetDeviceSyncSummaryLogs(context, serviceProviderId, DeviceSyncSummaryLogForTeal(serviceProviderId)));
                        var isTealUploaded = UploadDeviceSyncSummaryLogToS3(context, tealFile, "Teal-" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt");
                    }
                    break;
                case IntegrationType.Jasper:
                case IntegrationType.POD19:
                case IntegrationType.TMobileJasper:
                case IntegrationType.Rogers:
                    sqlTransientRetryPolicy.Execute(() => SyncJasperDevices(context, serviceProviderId));
                    if (DeviceSyncSummaryLogEnable)
                    {
                        var fileBytes = GenerateDeviceSyncSummaryLogFile(GetDeviceSyncSummaryLogsForJasper(context, serviceProviderId));
                        var isUploaded = UploadDeviceSyncSummaryLogToS3(context, fileBytes, "Jasper-" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt");
                    }
                    break;
                case IntegrationType.Pond:
                    sqlTransientRetryPolicy.Execute(() => SyncPondDevices(serviceProviderId, context));
                    if (DeviceSyncSummaryLogEnable)
                    {
                        var pondFile = GenerateDeviceSyncSummaryLogFile(GetDeviceSyncSummaryLogs(context, serviceProviderId, DeviceSyncSummaryLogForPond(serviceProviderId)));
                        var isPondUploaded = UploadDeviceSyncSummaryLogToS3(context, pondFile, "Pond-" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt");
                    }
                    break;
                default:
                    LogInfo(context, LogTypeConstant.Exception, LogCommonStrings.INTEGRATION_TYPE_IS_UNSUPPORTED);
                    throw new Exception(LogCommonStrings.INTEGRATION_TYPE_IS_UNSUPPORTED);
            }

            if (IntegrationTypeIsMobility(sqsValues.IntegrationType))
            {
                sqlTransientRetryPolicy.Execute(() => SyncCommonMobilityItems(context, sqsValues));
            }

            if (!IntegrationTypeIsMobility(sqsValues.IntegrationType))
            {
                sqlTransientRetryPolicy.Execute(() => SyncCommonM2MItems(context, sqsValues));
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "EF1001:Internal EF Core API usage.", Justification = "Despite the warning, referring to the package version for the time being is better than the alternative of copying and maintaining the code, license, etc.")]
        private static bool ShouldRetry(Exception exception)
        {
            // as of Microsoft.EntityFrameworkCore.SqlServer v3.1.7, DB Timeout (error number -2) is not considered a "transient" error, but for our purposes we will retry
            return SqlServerTransientExceptionDetector.ShouldRetryOn(exception) || IsSqlTimeout(exception);
        }

        private static bool IsSqlTimeout(Exception exception)
        {
            return exception is SqlException sqlException && sqlException.Number == SQL_ERROR_NUMBER_DB_TIMEOUT;
        }

        private IList<DeviceSyncSummaryLog> GetDeviceSyncSummaryLogsForJasper(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", $"GetDeviceSyncSummaryLogs({serviceProviderId})");
            var deviceSyncSummaryLogs = new List<DeviceSyncSummaryLog>();
            using (var jasperCn = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString))
            {
                using (var cn = new SqlConnection(context.CentralDbConnectionString))
                {
                    string query = $@"
                        SELECT DISTINCT
		                    [Device].[id],
                            [Device].[ICCID],
		                    [Device].[MSISDN],
		                    [Device].[ServiceProviderId],
		                    [JasperDeviceUsage].[CtdDataUsage] AS [RawDataUsage],
		                    [Device].[CtdDataUsage] AS [CurrentDataUsage],
		                    [JasperDeviceUsage].CtdSMSUsage AS RawSMSUsage,
		                    [Device].CtdSMSUsage AS [CurrentSMSUsage],
		                    [JasperDeviceUsage].CtdVoiceUsage AS [RawVoiceUsage],
		                    [Device].CtdVoiceUsage AS [CurrentVoiceUsage],
		                    [BillingPeriod].BillingCycleStartDate,
		                    [BillingPeriod].BillingCycleEndDate,
		                    CONVERT(datetime, CONVERT(date, DATEADD(DAY, -1, GETUTCDATE()))) AS [UsageDate]
                        FROM [Device]  
                        LEFT JOIN {jasperCn.Database}.[dbo].[JasperDevice] as [JasperDevice] ON [Device].[ICCID] = [JasperDevice].[ICCID] AND [Device].[ServiceProviderId] = [JasperDevice].[ServiceProviderId]
                        LEFT JOIN {jasperCn.Database}.[dbo].[JasperDeviceUsage] as [JasperDeviceUsage] ON [Device].[ICCID] = [JasperDeviceUsage].[ICCID] AND [Device].[ServiceProviderId] = [JasperDeviceUsage].[ServiceProviderId]
	                    LEFT JOIN [dbo].[BillingPeriod] ON [Device].BillingPeriodId = BillingPeriod.Id
                        WHERE [Device].[ServiceProviderId] = {serviceProviderId}
                    ";

                    using (var cmd = new SqlCommand(query, cn))
                    {
                        cn.Open();
                        SqlDataReader reader = cmd.ExecuteReader();

                        while (reader.Read())
                        {
                            deviceSyncSummaryLogs.Add(DeviceSyncSummaryLogFromReader(reader));
                        }

                        reader.Close();
                    }
                }

                return deviceSyncSummaryLogs;
            }
        }

        private IList<DeviceSyncSummaryLog> GetDeviceSyncSummaryLogs(KeySysLambdaContext context, int serviceProviderId, string query)
        {
            LogInfo(context, "SUB", $"GetDeviceSyncSummaryLogs({serviceProviderId})");
            var deviceSyncSummaryLogs = new List<DeviceSyncSummaryLog>();
            using (var cn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand(query, cn))
                {
                    cn.Open();
                    SqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        deviceSyncSummaryLogs.Add(DeviceSyncSummaryLogFromReader(reader));
                    }

                    reader.Close();
                }
            }

            return deviceSyncSummaryLogs;
        }

        private static string DeviceSyncSummaryLogForTelegence(int serviceProviderId)
        {
            return $@"
                SELECT DISTINCT
				[MobilityDevice].[id],
				[MobilityDevice].[ICCID],
				[MobilityDevice].[MSISDN],	
				[MobilityDevice].[ServiceProviderId],
				[TelegenceDevice].[CtdDataUsage] AS [RawDataUsage],
				[MobilityDevice].[CtdDataUsage] AS [CurrentDataUsage],
				[TelegenceDevice].[SMSCount] AS [RawSMSUsage],
				[MobilityDevice].[CtdSMSUsage] AS [CurrentSMSUsage],
				[TelegenceDevice].[MinutesUsed] AS [RawVoiceUsage],
				[MobilityDevice].[CtdVoiceUsage] AS [CurrentVoiceUsage],
				CONVERT(datetime, CONVERT(date, DATEADD(DAY, -1, GETUTCDATE()))) AS [UsageDate],
                [BillingPeriod].BillingCycleStartDate,
				[BillingPeriod].BillingCycleEndDate
			FROM [MobilityDevice]
			LEFT JOIN [dbo].[TelegenceDevice] ON [MobilityDevice].[MSISDN] = [TelegenceDevice].[SubscriberNumber] AND [MobilityDevice].[ServiceProviderId] = [TelegenceDevice].[ServiceProviderId]
			LEFT JOIN [dbo].[BillingPeriod] ON [MobilityDevice].BillingPeriodId = BillingPeriod.Id
			WHERE [MobilityDevice].[ServiceProviderId] = {serviceProviderId};
            ";
        }

        private static string DeviceSyncSummaryLogForThingSpace(int serviceProviderId)
        {
            return $@"
                    SELECT
                        [Device].[id],
			            [Device].[ICCID],
			            [Device].[MSISDN],
			            [Device].[MSISDN],
                        [ThingSpaceDeviceDailyUsage].[CtdDataUsage] AS [RawDataUsage],
			            [Device].[CtdDataUsage] AS [CurrentDataUsage],
                        [ThingSpaceDeviceDailyUsage].[CtdSMSUsage] AS [RawSMSUsage],
			            [Device].[CtdSMSUsage] AS [CurrentSMSUsage],
                        [ThingSpaceDeviceDailyUsage].[CtdVoiceUsage] AS [RawVoiceUsage],
			            [Device].[CtdVoiceUsage] AS [CurrentVoiceUsage],
                        [ThingSpaceDeviceDailyUsage].[CreatedDate] AS [UsageDate],
			            [BillingPeriod].BillingCycleStartDate,
			            [BillingPeriod].BillingCycleEndDate
                    FROM [Device]
                    INNER JOIN [ThingSpaceDeviceDailyUsage] ON [Device].[ICCID] = [ThingSpaceDeviceDailyUsage].[ICCID] AND [Device].[ServiceProviderId] = [ThingSpaceDeviceDailyUsage].[ServiceProviderId]
			        LEFT JOIN [dbo].[BillingPeriod] ON [Device].BillingPeriodId = BillingPeriod.Id
                    WHERE [Device].[ServiceProviderId] = {serviceProviderId} and [ThingSpaceDeviceDailyUsage].[CreatedDate] = CONVERT(datetime, CONVERT(date, DATEADD(DAY, -1, GETUTCDATE())))
                ";
        }

        private static string DeviceSyncSummaryLogForTeal(int serviceProviderId)
        {
            return $@"
                    SELECT
                        [Device].[id],
			            [Device].[ICCID],
			            [Device].[MSISDN],
			            [Device].[ServiceProviderId],
                        [TealDeviceUsageDailyStaging].[Usage] AS [RawDataUsage],
			            [Device].[CtdDataUsage] AS [CurrentDataUsage],
                        [TealDeviceUsageDailyStaging].[CreatedDate] AS [UsageDate],
			            [BillingPeriod].BillingCycleStartDate,
			            [BillingPeriod].BillingCycleEndDate,
                        NULL AS [RawSMSUsage],
                        NULL AS [CurrentSMSUsage],
                        NULL AS [RawVoiceUsage],
                        NULL AS [CurrentVoiceUsage]
                    FROM [TealDeviceUsageDailyStaging]
                    INNER JOIN [Device] ON [Device].[EID] = [TealDeviceUsageDailyStaging].[EID]
		            LEFT JOIN [dbo].[BillingPeriod] ON [Device].BillingPeriodId = BillingPeriod.Id
                    WHERE [Device].[ICCID] IS NOT NULL
                    AND [Device].[ServiceProviderId] = {serviceProviderId}
                    AND [Device].[Status] <> 'Unknown'
                ";
        }

        private static string DeviceSyncSummaryLogForPond(int serviceProviderId)
        {
            return $@"
                    SELECT 
                        [Device].[id],
			            [Device].[MSISDN],
                        [PondDevice].[ServiceProviderId],
                        [PondDevice].[ICCID],
                        ISNULL([CurrentPondDeviceUsage].[CloseTime], GETUTCDATE()) AS [UsageDate],
                        [CurrentPondDeviceUsage].[Duration] AS [RawDataUsage],
			            [Device].[ctdDataUsage] AS [CurrentDataUsage],
			            [BillingPeriod].BillingCycleStartDate,
			            [BillingPeriod].BillingCycleEndDate,
                        NULL AS [RawSMSUsage],
                        NULL AS [CurrentSMSUsage],
                        NULL AS [RawVoiceUsage],
                        NULL AS [CurrentVoiceUsage]
                    FROM [dbo].[PondDevice]
                    INNER JOIN [dbo].[Device] 
                        ON [Device].[IsDeleted] = 0 
                        AND [PondDevice].[ICCID] = [Device].[ICCID] 
                        AND [PondDevice].[ServiceProviderId] = [Device].[ServiceProviderId]
                    INNER JOIN (
                        SELECT 
                            [PondDeviceUsageStaging].[CloseTime],
                            [PondDeviceUsageStaging].[ServiceProviderId],
                            [PondDeviceUsageStaging].[ICCID],
                            [PondDeviceUsageStaging].[Duration]
                        FROM [dbo].[PondDeviceUsageStaging]
                    ) AS [CurrentPondDeviceUsage] ON [PondDevice].[ICCID] = [CurrentPondDeviceUsage].[ICCID] 
                        AND [PondDevice].[ServiceProviderId] = [CurrentPondDeviceUsage].[ServiceProviderId]
		            LEFT JOIN [dbo].[BillingPeriod] ON [Device].BillingPeriodId = BillingPeriod.Id
                    WHERE [PondDevice].[ICCID] IS NOT NULL
                    AND [PondDevice].[ServiceProviderId] = {serviceProviderId}
                    AND [PondDevice].[DeviceStatus] <> 'Unknown'
                ";
        }

        private static DeviceSyncSummaryLog DeviceSyncSummaryLogFromReader(SqlDataReader reader)
        {
            return new DeviceSyncSummaryLog
            {
                ID = reader["id"].ToString(),
                ICCID = reader["ICCID"].ToString(),
                MSISDN = reader["MSISDN"].ToString(),
                RawDataUsage = reader["RawDataUsage"] == DBNull.Value ? 0 : Convert.ToInt64(reader["RawDataUsage"]),
                CurrentDataUsage = reader["CurrentDataUsage"] == DBNull.Value ? 0 : Convert.ToInt64(reader["CurrentDataUsage"]),
                RawSMSUsage = reader["RawSMSUsage"] == DBNull.Value ? 0 : Convert.ToInt64(reader["RawSMSUsage"]),
                CurrentSMSUsage = reader["CurrentSMSUsage"] == DBNull.Value ? 0 : Convert.ToInt64(reader["CurrentSMSUsage"]),
                RawVoiceUsage = reader["RawVoiceUsage"] == DBNull.Value ? 0 : Convert.ToInt64(reader["RawVoiceUsage"]),
                CurrentVoiceUsage = reader["CurrentVoiceUsage"] == DBNull.Value ? 0 : Convert.ToInt64(reader["CurrentVoiceUsage"]),
                BillingCycleStartDate = reader["BillingCycleStartDate"] == DBNull.Value ? DateTime.MinValue : Convert.ToDateTime(reader["BillingCycleStartDate"]),
                BillingCycleEndDate = reader["BillingCycleEndDate"] == DBNull.Value ? DateTime.MinValue : Convert.ToDateTime(reader["BillingCycleEndDate"]),
                UsageDate = reader["UsageDate"] == DBNull.Value ? DateTime.MinValue : Convert.ToDateTime(reader["UsageDate"]),
                ServiceProviderId = reader["ServiceProviderId"] == DBNull.Value ? 0 : Convert.ToInt32(reader["ServiceProviderId"])
            };
        }

        private void SyncJasperDevices(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "PROC", "usp_Jasper_DeviceSync");
            using (var jasperCn = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString))
            {
                using (var cn = new SqlConnection(context.CentralDbConnectionString))
                {
                    cn.Open();

                    using (SqlTransaction transaction = cn.BeginTransaction())
                    {
                        using (var cmd = new SqlCommand("usp_Jasper_DeviceSync", cn, transaction))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                            cmd.Parameters.AddWithValue("@JasperDbName", jasperCn.Database);
                            cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);

                            try
                            {
                                cmd.ExecuteNonQuery();
                                // Commit transaction if all insertions succeed
                                transaction.Commit();
                            }
                            catch (Exception ex)
                            {
                                // Roll back the transaction if any error occurs
                                transaction.Rollback();
                                LogInfo(context, "EXCEPTION", $"Error during Device Usage Sync: {ex.Message} {ex.StackTrace}");
                            }
                        }
                    }
                }
            }
        }
        private void RecordJasperDevicesHistory(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "PROC", $"{serviceProviderId}");
            using (var cn = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Jasper_Device_Add_History", cn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = AMOPConstants.ShortTimeoutSeconds;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);

                    cn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void SyncThingSpaceDevices(int serviceProviderId, string connectionString, IKeysysLogger logger)
        {
            logger.LogInfo("PROC", "usp_ThingSpace_DeviceSync");
            using (var cn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand("usp_ThingSpace_DeviceSync", cn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 900;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);

                    cn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void SyncTealDevices(int serviceProviderId, KeySysLambdaContext context)
        {
            LogInfo(context, LogTypeConstant.Info, $"({serviceProviderId})");
            try
            {
                using (var conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var cmd = new SqlCommand(AMOPConstants.StoredProcedureName.usp_Teal_Device_Sync, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                        cmd.CommandTimeout = AMOPConstants.TimeoutSeconds;

                        conn.Open();

                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, ex.Message);
            }
        }

        private static void UpdateEbondingUsage(int serviceProviderId, string connectionString, IKeysysLogger logger)
        {
            logger.LogInfo("SUB", $"UpdateEbondingUsage({serviceProviderId})");
            using (var con = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand("usp_eBonding_Update_DeviceUsage_FromStaging", con))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 60;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);

                    con.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static void SyncEbondingDevices(int serviceProviderId, string connectionString, IKeysysLogger logger)
        {
            logger.LogInfo("PROC", "usp_eBonding_DeviceSync");
            using (var cn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand("usp_eBonding_DeviceSync", cn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 90;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);

                    cn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void SyncCommonMobilityItems(KeySysLambdaContext context, GetDevicesCleanupSqsValues sqsValues)
        {
            LogInfo(context, "PROC", "usp_MobilityDeviceSync_Common");
            using (var cn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_MobilityDeviceSync_Common", cn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 900;

                    if (sqsValues.IntegrationTypeReceived)
                    {
                        cmd.Parameters.AddWithValue("@IntegrationId", sqsValues.IntegrationType);
                    }

                    cn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void SyncCommonM2MItems(KeySysLambdaContext context, GetDevicesCleanupSqsValues sqsValues)
        {
            LogInfo(context, "PROC", "usp_DeviceSync_Common");
            using (var cn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_DeviceSync_Common", cn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 900;

                    if (sqsValues.IntegrationTypeReceived)
                    {
                        cmd.Parameters.AddWithValue("@IntegrationId", sqsValues.IntegrationType);
                    }

                    cn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static bool IntegrationTypeIsMobility(IntegrationType integrationType)
        {
            return integrationType == IntegrationType.eBonding ||
                integrationType == IntegrationType.Telegence;
        }

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
                default:
                    LogInfo(context, LogTypeConstant.Exception, LogCommonStrings.INTEGRATION_TYPE_IS_UNSUPPORTED);
                    throw new Exception(LogCommonStrings.INTEGRATION_TYPE_IS_UNSUPPORTED);
            }

            using (var cn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sqlText, cn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    if (integrationType == IntegrationType.Jasper
                        || integrationType == IntegrationType.POD19
                        || integrationType == IntegrationType.TMobileJasper
                        || integrationType == IntegrationType.Rogers)
                    {
                        cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    }

                    cn.Open();

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            if (!reader.IsDBNull(0))
                            {
                                summary.DetailLastSyncDate = reader.GetDateTime(0);
                            }

                            if (!reader.IsDBNull(1))
                            {
                                summary.DetailQueueCount = reader.GetInt32(1);
                            }

                            if (!reader.IsDBNull(2))
                            {
                                summary.DetailUpdatedCount = reader.GetInt32(2);
                            }

                            if (!reader.IsDBNull(3))
                            {
                                summary.UsageLastSyncDate = reader.GetDateTime(3);
                            }

                            if (!reader.IsDBNull(4))
                            {
                                summary.UsageQueueCount = reader.GetInt32(4);
                            }

                            if (!reader.IsDBNull(5))
                            {
                                summary.UsageUpdatedCount = reader.GetInt32(5);
                            }

                            if (!reader.IsDBNull(6))
                            {
                                summary.DeviceCount = reader.GetInt32(6);
                            }
                        }

                        reader.Close();
                    }

                    cn.Close();
                }
            }

            return summary;
        }

        private async Task SendEmailSummaryAsync(KeySysLambdaContext context, IntegrationType integrationType, DeviceSyncSummary summary, bool shouldGoToHistorian, IList<IntegrationTypeModel> integrationTypes)
        {
            try
            {
                LogInfo(context, "SUB", "SendEmailSummaryAsync()");
                LogInfo(context, "INFO", $"Send email summary Integration type: {integrationType}");
                LogInfo(context, "INFO", $"Send email summary General Provider Setting Email Subject: {context.GeneralProviderSettings.DeviceSyncResultsEmailSubject}");

                var integrationName = integrationTypes.FirstOrDefault(it => it.Id == (int)integrationType)?.Name;
                var subject = context.GeneralProviderSettings.DeviceSyncResultsEmailSubject.Replace("Jasper",
                    !string.IsNullOrEmpty(integrationName) ? integrationName : integrationType.ToString("G"));

                await SendEmailAsync(context, subject, BuildResultsEmailBody(context, summary, shouldGoToHistorian));
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION", $"Email unable to be sent: {ex.Message} {ex.StackTrace}");
            }
        }

        private async Task SendEmailAsync(KeySysLambdaContext context, string subject, BodyBuilder bodyBuilder)
        {
            LogInfo(context, "SUB", "SendEmailAsync()");
            var emailFactory = new SimpleEmailServiceFactory();
            using (var client = emailFactory.getClient(AwsSesCredentials(context), RegionEndpoint.USEast1))
            {
                var message = new MimeMessage();
                message.From.Add(MailboxAddress.Parse(context.GeneralProviderSettings.DeviceSyncFromEmailAddress));
                var recipientAddressList = context.GeneralProviderSettings.DeviceSyncToEmailAddresses.Split(';').ToList();
                foreach (var recipientAddress in recipientAddressList)
                {
                    if (!string.IsNullOrWhiteSpace(recipientAddress))
                    {
                        message.To.Add(MailboxAddress.Parse(recipientAddress));
                    }
                }

                message.Subject = subject;
                message.Body = bodyBuilder.ToMessageBody();
                using (var stream = new System.IO.MemoryStream())
                {
                    message.WriteTo(stream);

                    var sendRequest = new SendRawEmailRequest
                    {
                        RawMessage = new RawMessage(stream)
                    };
                    try
                    {
                        var response = await client.SendRawEmailAsync(sendRequest);
                        LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                    }
                    catch (Exception ex)
                    {
                        LogInfo(context, "EXCEPTION", "Error Sending Email: " + ex.Message);
                    }
                }
            }
        }

        private BodyBuilder BuildResultsEmailBody(KeySysLambdaContext context, DeviceSyncSummary summary, bool shouldGoToHistorian)
        {
            LogInfo(context, "SUB", "BuildResultsEmailBody()");
            string historianNote = shouldGoToHistorian ? "Devices sent to Historian" : string.Empty;
            if (summary == null)
            {
                return new BodyBuilder
                {
                    HtmlBody = "<h1>Device Sync Summary</h1><h2>Error getting device summary</h2>",
                    TextBody = $"Device Sync Summary{Environment.NewLine}Error getting device summary"
                };
            }

            return new BodyBuilder
            {
                HtmlBody = $"<h1>Device Sync Summary</h1><h2>Device Details</h2><div><b>Last Sync:</b> {(summary.DetailLastSyncDate != null ? summary.DetailLastSyncDate.Value.ToShortDateString() : "")}<br/><b>Queue Count:</b> {summary.DetailQueueCount.GetValueOrDefault(0)}<br/><b>Update Count:</b> {summary.DetailUpdatedCount.GetValueOrDefault(0)}</div><h2>Device Usage</h2><div><b>Last Sync:</b> {(summary.UsageLastSyncDate != null ? summary.UsageLastSyncDate.Value.ToShortDateString() : "")}<br/><b>Queue Count:</b> {summary.UsageQueueCount.GetValueOrDefault(0)}<br/><b>Update Count:</b> {summary.UsageUpdatedCount.GetValueOrDefault(0)}</div><div>{historianNote}</div>",
                TextBody = $"Device Sync Summary{Environment.NewLine}Device Details{Environment.NewLine}Last Sync: {(summary.DetailLastSyncDate != null ? summary.DetailLastSyncDate.Value.ToShortDateString() : "")}{Environment.NewLine}Queue Count: {summary.DetailQueueCount.GetValueOrDefault(0)}{Environment.NewLine}Update Count: {summary.DetailUpdatedCount.GetValueOrDefault(0)}{Environment.NewLine}Device Usage{Environment.NewLine}Last Sync: {(summary.UsageLastSyncDate != null ? summary.UsageLastSyncDate.Value.ToShortDateString() : "")}{Environment.NewLine}Queue Count: {summary.UsageQueueCount.GetValueOrDefault(0)}{Environment.NewLine}Update Count: {summary.UsageUpdatedCount.GetValueOrDefault(0)}{Environment.NewLine}{historianNote}"
            };
        }

        private async Task SendNotificationMessageToQueue(KeySysLambdaContext context, GetDevicesCleanupSqsValues sqsValues, int retryCount, int remainingRowsToProcess, IntegrationType integrationType)
        {
            LogInfo(context, "SUB", "SendNotificationMessageToQueue");
            LogInfo(context, "DeviceNotificationQueueURL", DeviceNotificationQueueURL);
            LogInfo(context, "RemainingRowsToProcess", remainingRowsToProcess);
            LogInfo(context, "RetryCount", retryCount);
            LogInfo(context, "IntegrationType", integrationType);
            LogInfo(context, "MaxRetries", sqsValues.MaxRetries);
            LogInfo(context, "DelayBetweenRetries", sqsValues.DelaySeconds);

            if (string.IsNullOrWhiteSpace(DeviceNotificationQueueURL))
            {
                return; // to be able to skip enqueuing messages in a test
            }

            using (var client = new AmazonSQSClient(AwsCredentials(context), RegionEndpoint.USEast1))
            {
                var request = new SendMessageRequest
                {
                    DelaySeconds = sqsValues.DelaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {"RetryCount", new MessageAttributeValue {DataType = "Number", StringValue = retryCount.ToString()}},
                        {"IntegrationType", new MessageAttributeValue {DataType = "Number", StringValue = ((int)integrationType).ToString()}},
                        {"ServiceProviderId", new MessageAttributeValue {DataType = "Number", StringValue = sqsValues.ServiceProviderId.ToString()}},
                        {"DelayBetweenRetries", new MessageAttributeValue {DataType = "Number", StringValue = sqsValues.DelaySeconds.ToString()}},
                        {"MaxRetries", new MessageAttributeValue {DataType = "Number", StringValue = sqsValues.MaxRetries.ToString()}},
                        {"RemainingRowsToProcess", new MessageAttributeValue {DataType = "Number", StringValue = remainingRowsToProcess.ToString()}}
                    },
                    MessageBody = "Sending device sync notification",
                    QueueUrl = DeviceNotificationQueueURL,
                };
                LogInfo(context, "MessageBody", request.MessageBody);

                var response = await client.SendMessageAsync(request);
                LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
            }
        }

        private void UpdateTelegenceDevicesFromStaging(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", "UpdateTelegenceDevicesFromStaging");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Telegence_Update_Device", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.CommandTimeout = 1800;
                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateTelegenceDeviceDetailsFromStaging(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", "UpdateTelegenceDeviceDetailsFromStaging");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Telegence_Update_DeviceDetail", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.CommandTimeout = 1800;
                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateTelegenceUsageFromStaging(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", "UpdateTelegenceUsageFromStaging");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Telegence_Update_DeviceUsage_FromStaging", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.CommandTimeout = 1800;
                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateMobilityFeatureFromStaging(KeySysLambdaContext context)
        {
            LogInfo(context, "SUB", "UpdateTelegenceUsageFromStaging");
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_TelegenceDeviceMobilityFeature_FromStaging", con)
                {
                    CommandType = CommandType.StoredProcedure
                })
                {
                    con.Open();
                    cmd.CommandTimeout = 800;
                    cmd.ExecuteNonQuery();
                }

                con.Close();
            }
        }

        private void UpdateTelegenceMubuUsageFromStaging(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", "UpdateTelegenceMubuUsageFromStaging");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Telegence_Update_DeviceMubuUsage_FromStaging", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.CommandTimeout = 1800;
                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void RunTelegenceDeviceSync(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", "RunTelegenceDeviceSync");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Telegence_DeviceSync", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.CommandTimeout = 1800;

                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void SyncPondDevices(int serviceProviderId, KeySysLambdaContext context)
        {
            LogInfo(context, CommonConstants.INFO, $"({serviceProviderId})");
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = new SqlCommand(AMOPConstants.StoredProcedureName.POND_DEVICE_SYNC, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                        command.CommandTimeout = AMOPConstants.TimeoutSeconds;
                        connection.Open();
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, ex.Message);
            }
        }

        private byte[] GenerateDeviceSyncSummaryLogFile(IList<DeviceSyncSummaryLog> summaries)
        {
            using (var ms = new MemoryStream())
            using (var sw = new StreamWriter(ms))
            {
                sw.WriteLine("ID,ICCID,MSISDN,RawDataUsage,CurrentDataUsage,RawSMSUsage,CurrentSMSUsage,RawVoiceUsage,CurrentVoiceUsage,BillingCycleStartDate,BillingCycleEndDate,UsageDate,ServiceProviderId");

                foreach (var item in summaries)
                {
                    sw.WriteLine(
                        $"{item.ID},{item.ICCID},{item.MSISDN},{item.RawDataUsage},{item.CurrentDataUsage},{item.RawSMSUsage},{item.CurrentSMSUsage},{item.RawVoiceUsage},{item.CurrentVoiceUsage},{item.BillingCycleStartDate.GetValueOrDefault():yyyy-MM-dd},{item.BillingCycleEndDate.GetValueOrDefault():yyyy-MM-dd},{item.UsageDate.GetValueOrDefault():yyyy-MM-dd},{item.ServiceProviderId}");
                }

                sw.Flush();

                var fileBytes = new byte[ms.Length];
                ms.Position = 0;
                ms.Read(fileBytes, 0, fileBytes.Length);

                sw.Close();

                return fileBytes;
            }
        }

        private async Task<bool> UploadDeviceSyncSummaryLogToS3(KeySysLambdaContext context, byte[] fileBytes, string fileName)
        {
            try
            {
                LogInfo(context, "SUB", "UploadDeviceSyncSummaryLogToS3()");
                LogInfo(context, "BucketName", DeviceSyncSummaryLogS3BucketName);
                LogInfo(context, "FileName", fileName);

                s3Wrapper.UploadAwsFile(fileBytes, fileName);

                var statusUploadFileToS3 = await s3Wrapper.WaitForFileUploadCompletion(fileName, CommonConstants.DELAY_IN_SECONDS_FIVE_MINUTES, context.logger);
                var statusUploadFile = (IsUploadSuccess: statusUploadFileToS3.Item1, ErrorMessage: statusUploadFileToS3.Item2);
                if (!statusUploadFile.IsUploadSuccess)
                {
                    LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.UPLOAD_FILE_TO_S3_NOT_SUCCESS, $"{fileName}") + " " + statusUploadFile.ErrorMessage);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogInfo(context, CommonConstants.WARNING, $"Error Uploading File to S3: {ex.Message}");
                return false;
            }
        }
    }
}
