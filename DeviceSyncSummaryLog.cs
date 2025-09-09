using System;
using System.Runtime.CompilerServices;

namespace AltaworxJasperGetDevicesCleanup
{
    public class DeviceSyncSummaryLog
    {
        public string ID { get; set; }
        public string ICCID { get; set; }
        public string MSISDN { get; set; }
        public long? RawDataUsage { get; set; }
        public long? CurrentDataUsage { get; set; }
        public long? RawSMSUsage { get; set; }
        public long? CurrentSMSUsage { get; set; }
        public long? RawVoiceUsage { get; set; }
        public long? CurrentVoiceUsage { get; set; }
        public DateTime? BillingCycleStartDate { get; set; }
        public DateTime? BillingCycleEndDate { get; set; }
        public DateTime? UsageDate { get; set; }
        public int ServiceProviderId { get; set; }
    }
}
