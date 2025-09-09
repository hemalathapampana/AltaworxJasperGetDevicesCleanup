using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Altaworx.AWS.Core.Models;
using Amazon;
using Amazon.SimpleEmail.Model;
using Amop.Core.Constants;
using Amop.Core.Repositories.Optimization;
using MimeKit;

namespace Altaworx.AWS.Core.Services.Optimization
{
    public static class OptimizationUsageSyncErrorHandler
    {
        public static async Task ProcessStopCarrierOptimization(KeySysLambdaContext context, int serviceProviderId, long optimizationSessionId, string errorMessage)
        {
            AwsFunctionBase.LogInfo(context, CommonConstants.SUB, "");
            AwsFunctionBase.LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOPPING_OPTIMIZATION_SESSION_MESSAGE, optimizationSessionId));
            await SendOptimizationSyncDeviceNotificationEmail(context, serviceProviderId, errorMessage);
            var optimizationRepository = new OptimizationRepository();
            optimizationRepository.RemoveCarrierOptimizationSession(context.CentralDbConnectionString, optimizationSessionId, context.logger);
        }

        private static async Task SendOptimizationSyncDeviceNotificationEmail(KeySysLambdaContext context, int serviceProviderId, string errorMessage)
        {
            AwsFunctionBase.LogInfo(context, CommonConstants.SUB, "");

            var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, serviceProviderId);
            var emailSubject = string.Format(context.OptimizationSettings.OptimizationSyncDeviceErrorEmailSubject, serviceProvider.Name);
            var emailBody = BuildOptimizationSyncDeviceNotificationEmailBody(context, errorMessage, serviceProvider.Name);
            try
            {
                await SendOptimizationUsageSyncEmailAsync(context, emailSubject, emailBody);
            }
            catch (Exception ex)
            {
                AwsFunctionBase.LogInfo(context, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
        }

        private static BodyBuilder BuildOptimizationSyncDeviceNotificationEmailBody(KeySysLambdaContext context, string errorMessage, string serviceProviderName)
        {
            AwsFunctionBase.LogInfo(context, CommonConstants.SUB, "");
            return new BodyBuilder
            {
                HtmlBody = $@"<html>
                                <h2>{LogCommonStrings.CARRIER_OPTIMIZATION_SYNC_DEVICE_ERROR}</h2>
                                <p>{string.Format(LogCommonStrings.CARRIER_OPTIMIZATION_SYNC_DEVICE_ERROR_EMAIL_BODY, serviceProviderName)}</p>
                                <p>{LogCommonStrings.ERROR_DETAILS}: {errorMessage}</p>
                              </html>",
                TextBody = $@"{LogCommonStrings.CARRIER_OPTIMIZATION_SYNC_DEVICE_ERROR} - {string.Format(LogCommonStrings.CARRIER_OPTIMIZATION_SYNC_DEVICE_ERROR_EMAIL_BODY, serviceProviderName)} - {LogCommonStrings.ERROR_DETAILS}: {errorMessage}"
            };
        }

        private static async Task SendOptimizationUsageSyncEmailAsync(KeySysLambdaContext context, string subject, BodyBuilder bodyBuilder)
        {
            AwsFunctionBase.LogInfo(context, CommonConstants.SUB, "");
            var emailFactory = new SimpleEmailServiceFactory();
            using (var client = emailFactory.getClient(AwsFunctionBase.AwsSesCredentials(context), RegionEndpoint.USEast1))
            {
                var awsEnvironment = context.EnvironmentRepo.GetEnvironmentVariable(context.Context, CommonConstants.AWS_ENV);
                var emailSender = new EmailSender(client, context.logger, awsEnvironment);
                var senderAddress = context.GeneralProviderSettings.DeviceSyncFromEmailAddress;
                var recipientAddressList = context.GeneralProviderSettings.DeviceSyncToEmailAddresses.Split(';').ToList();
                await emailSender.SendEmailAsync(senderAddress, recipientAddressList, subject, bodyBuilder);
            }
        }
    }
}
