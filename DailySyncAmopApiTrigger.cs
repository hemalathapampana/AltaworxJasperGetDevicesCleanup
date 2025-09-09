using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Altaworx.AWS.Core.Models;
using Amazon.Lambda.Core;
using Amop.Core.Constants;
using Amop.Core.Logger;
using Amop.Core.Repositories.Environment;

namespace Altaworx.AWS.Core
{
    public class DailySyncAmopApiTrigger
    {
        private string amop20SyncUpdateApiUrl;
        private readonly EnvironmentRepository environmentRepo = new EnvironmentRepository();
        public void SendNotificationToAmop20(KeySysLambdaContext context, ILambdaContext Lambda_context, string keyName, int? tenantId = null, string tenantName = null)
        {
            amop20SyncUpdateApiUrl = AwsFunctionBase.GetStringValueFromEnvironmentVariable(Lambda_context, environmentRepo, CommonConstants.AMOP_20_SYNC_UPDATE_API_URL_KEY);
            using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
            {
                client.BaseAddress = new Uri(amop20SyncUpdateApiUrl);
                client.DefaultRequestHeaders.Add("Accept", "application/json");
                string jsonRequest = null;
                jsonRequest = "{\"data\": { \"key_name\": \"" + keyName + "\",\"tenant_id\": \"" + tenantId + "\",\"tenant_name\": \"" + tenantName + "\"}}";
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
    }
}
