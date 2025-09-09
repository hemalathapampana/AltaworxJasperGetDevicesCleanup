using System;

namespace AltaworxJasperGetDevicesCleanup
{
    public class DeviceSyncSummary
    {
        public DateTime? DetailLastSyncDate { get; set; }
        public int? DetailQueueCount { get; set; }
        public int? DetailUpdatedCount { get; set; }
        public DateTime? UsageLastSyncDate { get; set; }
        public int? UsageQueueCount { get; set; }
        public int? UsageUpdatedCount { get; set; }
        public int? DeviceCount { get; set; }
    }
}
