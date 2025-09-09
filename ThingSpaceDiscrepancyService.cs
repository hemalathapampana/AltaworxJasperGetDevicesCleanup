using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Services;
using Altaworx.AWS.Core.Services.Email;
using Amazon;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Resilience;
using Microsoft.Data.SqlClient;
using OfficeOpenXml;

namespace AltaworxJasperGetDevicesCleanup
{
    public class ThingSpaceDiscrepancyService
    {
        public async Task CheckDevicesForBillingPeriodDiscrepancy(KeySysLambdaContext context, GetDevicesCleanupSqsValues sqsValues)
        {
            // Call Stored procedure here to get devices with incorrect billing period if active
            var devices = GetDevicesWithBillingCycleDiscrepancy(context, sqsValues.ServiceProviderId);
            if (devices.Count == 0)
            {
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, LogCommonStrings.NO_DEVICE_DISCREPANCY_FOUND);
                return;
            }
            // Get the result from Stored procedure to put into an Excel file
            byte[] fileByte;
            using (ExcelPackage package = new ExcelPackage())
            {
                ExcelWorksheet worksheet = package.Workbook.Worksheets.Add(CommonConstants.INCORRECT_BILL_CYCLE_DEVICES);
                worksheet.Cells["A1"].LoadFromCollection(devices, true);
                fileByte = package.GetAsByteArray();
            }
            // Build email message and send it
            var emailFactory = new SimpleEmailServiceFactory();
            using var client = emailFactory.getClient(
                context.GeneralProviderSettings.AwsSesCredentials, RegionEndpoint.USEast1);
            var awsEnvironment = context.EnvironmentRepo.GetEnvironmentVariable(context.Context, CommonConstants.AWS_ENV);
            var emailService = new DeviceDiscrepancyEmailService(context, new EmailSender(client, context.logger, awsEnvironment));
            var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, sqsValues.ServiceProviderId);
            var policyFactory = new PolicyFactory(context.logger);
            var sqlRetryPolicy = policyFactory.GetSqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES);
            var portalConnectionString = context.EnvironmentRepo.GetEnvironmentVariable(context.Context, EnvironmentVariableKeyConstants.PORTAL_CONNECTION_STRING);
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.TENANT_ID, serviceProvider.TenantId),
            };
            var tenantName = sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(context),
                    portalConnectionString,
                    Amop.Core.Constants.SQLConstant.StoredProcedureName.GET_TENANT_NAME_BY_ID,
                    (dataReader) => ReadTenantName(dataReader),
                    parameters,
                    Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds).FirstOrDefault());

            await emailService.BuildAndSendEmailAsync(serviceProvider.DisplayName, tenantName, fileByte);
        }

        private string ReadTenantName(SqlDataReader dataReader)
        {
            var columns = dataReader.GetColumnsFromReader();
            return dataReader.StringFromReader(columns, CommonColumnNames.Name);
        }

        public static Action<string, string> ParameterizedLog(KeySysLambdaContext context)
        {
            return (type, message) => AwsFunctionBase.LogInfo(context, type, message);
        }

        public List<IncorrectBillCycleDevice> GetDevicesWithBillingCycleDiscrepancy(KeySysLambdaContext context, int serviceProviderId)
        {
            var devices = new List<IncorrectBillCycleDevice>();
            try
            {
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Sub, $"serviceProviderId: {serviceProviderId}");

                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.usp_GetDevicesWithBillingCycleDiscrepancy;
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds;
                        command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                        connection.Open();

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    var device = IncorrectBillCycleDevice.FromReader(reader);
                                    devices.Add(device);
                                }
                            }
                            else
                            {
                                AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, LogCommonStrings.NO_DEVICE_DISCREPANCY_FOUND);
                            }
                        }
                    }
                }

                return devices;
            }
            catch (SqlException ex)
            {
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_STORED_PROCEDURE, ex.Message, ex.ErrorCode, ex.Number, ex.StackTrace));
            }
            catch (InvalidOperationException ex)
            {
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Exception, ex.Message);
            }
            return devices;
        }
    }
}
