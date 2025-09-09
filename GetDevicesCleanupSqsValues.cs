using System;
using System.Collections.Generic;
using System.Text;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Amazon.Lambda.SQSEvents;
using Amop.Core.Models;

namespace AltaworxJasperGetDevicesCleanup
{
    public class GetDevicesCleanupSqsValues
    {
        private const int MAX_RETRIES_DEFAULT = 14;
        private const int DELAY_SECONDS_DEFAULT = 300;

        public int RetryCount { get; private set; }
        public int? RemainingRowsToProcess { get; private set; }
        public int MaxRetries = MAX_RETRIES_DEFAULT;
        public int DelaySeconds = DELAY_SECONDS_DEFAULT;
        public IntegrationType IntegrationType { get; private set; } = IntegrationType.Jasper;
        public bool IntegrationTypeReceived { get; private set; }
        public int ServiceProviderId { get; private set; }
        public bool ShouldQueueCarrierOptimization { get; private set; }
        public string SnowflakeS3BucketName { get; private set; }
        public string SnowflakeS3BucketPath { get; private set; }
        public long OptimizationSessionId { get; private set; }

        public GetDevicesCleanupSqsValues(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            if (message.MessageAttributes.ContainsKey("RetryCount"))
            {
                RetryCount = Convert.ToInt32(message.MessageAttributes["RetryCount"].StringValue);
                context.LogInfo("RetryCount", RetryCount);
            }

            if (message.MessageAttributes.ContainsKey("RemainingRowsToProcess"))
            {
                RemainingRowsToProcess = Convert.ToInt32(message.MessageAttributes["RemainingRowsToProcess"].StringValue);
                context.LogInfo("RemainingRowsToProcess", RemainingRowsToProcess);
            }

            if (message.MessageAttributes.ContainsKey("MaxRetries"))
            {
                MaxRetries = Convert.ToInt32(message.MessageAttributes["MaxRetries"].StringValue);
                context.LogInfo("MaxRetries", MaxRetries);
            }
            else
            {
                MaxRetries = MAX_RETRIES_DEFAULT;
            }

            if (message.MessageAttributes.ContainsKey("DelayBetweenRetries"))
            {
                DelaySeconds = Convert.ToInt32(message.MessageAttributes["DelayBetweenRetries"].StringValue);
                context.LogInfo("DelayBetweenRetries", DelaySeconds);
            }
            else
            {
                DelaySeconds = DELAY_SECONDS_DEFAULT;
            }

            if (message.MessageAttributes.ContainsKey("IntegrationType"))
            {
                IntegrationType = (IntegrationType)Convert.ToInt32(message.MessageAttributes["IntegrationType"].StringValue);
                IntegrationTypeReceived = true;
                context.LogInfo("IntegrationType", IntegrationType);
            }

            if (message.MessageAttributes.ContainsKey("ServiceProviderId"))
            {
                ServiceProviderId = Convert.ToInt32(message.MessageAttributes["ServiceProviderId"].StringValue);
                context.LogInfo("ServiceProviderId", ServiceProviderId);
            }

            if (message.MessageAttributes.ContainsKey("ShouldQueueCarrierOptimization"))
            {
                ShouldQueueCarrierOptimization = Convert.ToBoolean(message.MessageAttributes["ShouldQueueCarrierOptimization"].StringValue);
                context.LogInfo("ShouldQueueCarrierOptimization", ShouldQueueCarrierOptimization);
            }

            if (message.MessageAttributes.ContainsKey("OptimizationSessionId"))
            {
                OptimizationSessionId = Convert.ToInt64(message.MessageAttributes["OptimizationSessionId"].StringValue);
                context.LogInfo("OptimizationSessionId", OptimizationSessionId);
            }
            else
            {
                OptimizationSessionId = 0;
            }

            SnowflakeS3BucketName = context.EnvironmentRepo.GetEnvironmentVariable(context.Context, "SnowflakeS3BucketName");
            context.LogInfo("SnowflakeS3BucketName", SnowflakeS3BucketName);

            SnowflakeS3BucketPath = context.EnvironmentRepo.GetEnvironmentVariable(context.Context, "SnowflakeS3BucketPath");
            context.LogInfo("SnowflakeS3BucketPath", SnowflakeS3BucketPath);
        }
    }
}
