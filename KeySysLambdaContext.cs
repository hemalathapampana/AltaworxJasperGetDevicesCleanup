using System;
using System.Collections.Generic;
using System.Text;
using Altaworx.AWS.Core.Helpers.Constants;
using Amazon.Lambda.Core;
using Amop.Core.Logger;
using Amop.Core.Models.Settings;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Tenant;
using Amop.Core.Services.Base64Service;
using StackExchange.Redis;

namespace Altaworx.AWS.Core.Models
{
    public class KeySysLambdaContext
    {
        public ILambdaContext Context { get; private set; }

        public string CentralDbConnectionString = null;
        public string BaseMultiTenantConnectionString = null;
        public bool VerboseLogging = false;
        public string BillingPeriodMonthString = string.Empty;
        public string BillingPeriodYearString = string.Empty;

        public IKeysysLogger logger { get; private set; }
        public IEnvironmentRepository EnvironmentRepo { get; private set; }
        public ISettingsRepository SettingsRepo { get; private set; }
        public ITenantRepository TenantRepo { get; private set; }
        public IBase64Service Base64Service { get; set; }
        public string LogDetails { get; set; }
        public bool IsProduction { get; set; }
        public int? BillingPeriodMonth { get; set; }
        public int? BillingPeriodYear { get; set; }
        public GeneralProviderSettings GeneralProviderSettings { get; set; }
        public OptimizationSettings OptimizationSettings { get; set; }

        public bool SkipOUSpecificLogic { get; set; }

        public string RedisConnectionString = string.Empty;
        public ConnectionMultiplexer CacheConnectionMultiplexer { get; set; }

        public bool IsRedisConnectionStringValid
        {
            get
            {
                return !string.IsNullOrEmpty(RedisConnectionString)
                        && RedisConnectionString != RedisCacheConstant.EmptyConnectionString;
            }
        }

        public bool IsCacheConnected
        {
            get
            {
                return RedisConnectionString != null
                       && CacheConnectionMultiplexer != null
                       && CacheConnectionMultiplexer.IsConnected;
            }
        }

        public KeySysLambdaContext(ILambdaContext context)
            : this(context, false)
        {
        }

        public KeySysLambdaContext(ILambdaContext context, bool skipOUSpecificLogic)
        {
            SkipOUSpecificLogic = skipOUSpecificLogic;
            Context = context;

            InitializeContext(SkipOUSpecificLogic);
        }

        public void InitializeContext(bool skipOUSpecificLogic)
        {
            EnvironmentRepo = new EnvironmentRepository();
            logger = new KeysysLambdaLogger(Context.Logger, Context, EnvironmentRepo);
            LogInfo(logger, "VERBOSE LOGGING", VerboseLogging);

            Base64Service = new Base64Service();

            SkipOUSpecificLogic = skipOUSpecificLogic;

            IsProduction = Context != null && Context.InvokedFunctionArn != null && Context.InvokedFunctionArn.EndsWith(":PROD");

            CentralDbConnectionString = EnvironmentRepo.GetEnvironmentVariable(Context, "ConnectionString");
            BaseMultiTenantConnectionString = EnvironmentRepo.GetEnvironmentVariable(Context, "BaseMultiTenantConnectionString");
            VerboseLogging = Convert.ToBoolean(EnvironmentRepo.GetEnvironmentVariable(Context, "VerboseLogging"));
            BillingPeriodMonthString = EnvironmentRepo.GetEnvironmentVariable(Context, "BillingPeriodMonth");
            BillingPeriodYearString = EnvironmentRepo.GetEnvironmentVariable(Context, "BillingPeriodYear");

            RedisConnectionString = EnvironmentRepo.GetEnvironmentVariable(Context, "RedisConnectionString");

            SettingsRepo = new SettingsRepository(logger, CentralDbConnectionString, Base64Service);
            TenantRepo = new TenantRepository(BaseMultiTenantConnectionString);

            if (!skipOUSpecificLogic)
            {
                LoadOUSettings();
            }
        }

        public void LogInfo(string desc, object detail)
        {
            LogInfo(logger, desc, detail);
        }

        public static void LogInfo(IKeysysLogger logger, string desc, object detail)
        {
            logger.LogInfo(desc, detail);
        }

        public void LoadOUSettings()
        {
            LogInfo(logger, "SUB", "LoadOUSettings");
            LoadOptimizationSettings(SettingsRepo);
            GeneralProviderSettings = SettingsRepo.GetGeneralProviderSettings();
        }

        private void LoadOptimizationSettings(ISettingsRepository settingsRepository)
        {
            LogInfo(logger, "SUB", "LoadOptimizationSettings");

            // Initialize context with optimization settings of the parent tenant since we don't always have access to the current tenantId
            OptimizationSettings = settingsRepository.GetOptimizationSettings();

            if (!string.IsNullOrWhiteSpace(BillingPeriodMonthString) && !string.IsNullOrWhiteSpace(BillingPeriodYearString))
            {
                int tempBillingPeriodMonth = 0;
                int tempBillingPeriodYear = 0;
                if (int.TryParse(BillingPeriodMonthString, out tempBillingPeriodMonth) && int.TryParse(BillingPeriodYearString, out tempBillingPeriodYear))
                {
                    BillingPeriodMonth = tempBillingPeriodMonth;
                    BillingPeriodYear = tempBillingPeriodYear;
                }
            }
        }

        public void CleanUp()
        {
            logger.Flush();
        }


        public bool ConnectToRedisCache()
        {
            //need to connect & disconnect since the StackExchange.Redis package is using up memory if we keep the ConnectionMultiplexer
            if (IsRedisConnectionStringValid)
            {
                ConfigurationOptions cacheOptions = ConfigurationOptions.Parse(RedisConnectionString);
                cacheOptions.AbortOnConnectFail = false;
                CacheConnectionMultiplexer = ConnectionMultiplexer.Connect(cacheOptions);
                LogInfo("INFO", "Connected to Redis cache.");
                return true;
            }
            return false;
        }

        public bool DisconnectFromRedisCache()
        {
            if (CacheConnectionMultiplexer != null)
            {
                CacheConnectionMultiplexer.Close();
                CacheConnectionMultiplexer = null;
                LogInfo("INFO", "Disconnected from Redis cache.");
                return true;
            }
            return false;
        }

        public bool TestRedisConnection()
        {
            if (!IsRedisConnectionStringValid)
            {
                return false;
            }

            //connect to cache
            ConfigurationOptions cacheOptions = ConfigurationOptions.Parse(RedisConnectionString);
            cacheOptions.AbortOnConnectFail = false;
            CacheConnectionMultiplexer = ConnectionMultiplexer.Connect(cacheOptions);

            //check if cache is valid
            var isRedisCacheReachable = CacheConnectionMultiplexer.IsConnected;

            //disconnect from cache
            CacheConnectionMultiplexer.Close();
            CacheConnectionMultiplexer = null;
            return isRedisCacheReachable;
        }

        public void LoadOptimizationSettingsByTenantId(int tenantId)
        {
            if (tenantId > 0)
            {
                OptimizationSettings = SettingsRepo.GetOptimizationSettings(tenantId);
            }
        }
    }
}
