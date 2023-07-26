using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Azure.Identity;
using Azure.Monitor.Query;
using System.Linq;
using Azure;
using Azure.Monitor.Query.Models;
using Azure.ResourceManager.PowerBIDedicated;
using Azure.ResourceManager;
using Azure.ResourceManager.Resources;
using Newtonsoft.Json;

namespace AutoPausePowerBIEmbedded
{
    public class CheckMetrics
    {
        [FunctionName("CheckMetrics")]
        public void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            // Verify required environment variables are set
            if(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_SUBSCRIPTION_ID")) ||
               string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_RESOURCE_GROUP_NAME")) ||
               string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_PBI_EMBEDDED_CAPACITY_NAME")))
            {
                log.LogError("AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP_NAME, and AZURE_PBI_EMBEDDED_CAPACITY_NAME application settings must be set.");
                return;
            }
            /****************Configuration***************/
            /* Azure Information                        */
            string subscriptionId = Environment.GetEnvironmentVariable("AZURE_SUBSCRIPTION_ID");
            string resourceGroupName = Environment.GetEnvironmentVariable("AZURE_RESOURCE_GROUP_NAME");
            string capacityName = Environment.GetEnvironmentVariable("AZURE_PBI_EMBEDDED_CAPACITY_NAME");
            /* Timing Information                        */
            // If not set default to 45 minutes
            int idleMinutesBeforePause = Environment.GetEnvironmentVariable("IDLE_MINUTES_BEFORE_PAUSE") is null 
                ? 45 
                : Convert.ToInt32(Environment.GetEnvironmentVariable("IDLE_MINUTES_BEFORE_PAUSE"));
            // If not set default to 30 minutes
            int minutesAfterResumeBeforeCheck = Environment.GetEnvironmentVariable("MINUTES_AFTER_RESUME_BEFORE_IDLE_CHECK") is null 
                ? 30 
                : Convert.ToInt32(Environment.GetEnvironmentVariable("MINUTES_AFTER_RESUME_BEFORE_IDLE_CHECK"));


            string resourceId = $"/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.PowerBIDedicated/capacities/{capacityName}";

            // Here we are using a raw response as the current SDK does not support the status information
            string capacity_state = string.Empty;
            Azure.Response<DedicatedCapacityResource> capacity = null;
            try
            {
                // Set up our client to get ARM information about the capacity
                var arm_client = new ArmClient(new DefaultAzureCredential(), subscriptionId);

                // Get our embedded capacity information by connecting to the subscription and resource group
                SubscriptionResource subscription = arm_client.GetDefaultSubscription();
                ResourceGroupResource resourceGroup = subscription.GetResourceGroups().Get(resourceGroupName);

                capacity = resourceGroup.GetDedicatedCapacities().Get(capacityName);
                var response = capacity.GetRawResponse();
                dynamic cap_raw = JsonConvert.DeserializeObject<dynamic>(response.Content.ToString());
                capacity_state = cap_raw.properties.state.ToString();
            } catch (Exception e)
            {
                log.LogError($"Error getting capacity information: {e.Message}");
                return;
            }
            if (capacity_state == "Succeeded")
            {
                // using the Log Query to get the Activity logs for the capacity to determine if it has been resumed 
                // within the MINUTES_AFTER_RESUME_BEFORE_IDLE_CHECK time frame, no need to check if it hasn't been up that long
                bool capacity_resumed = false;
                try
                {
                    var log_client = new LogsQueryClient(new DefaultAzureCredential());
                    string activity_query = "AzureActivity | where ActivityStatusValue == 'Success' and OperationNameValue == 'MICROSOFT.POWERBIDEDICATED/CAPACITIES/RESUME/ACTION' | project TimeGenerated, OperationName, ResourceGroup, Caller";
                    var activity_results = log_client.QueryResource(new Azure.Core.ResourceIdentifier(resourceId), activity_query, new QueryTimeRange(TimeSpan.FromMinutes(minutesAfterResumeBeforeCheck)));
                    capacity_resumed = activity_results.Value.Table.Rows.Count == 0;
                } catch (Exception e) {
                    log.LogError($"Error getting activity logs: {e.Message}");
                    return;
                }
                
                if(capacity_resumed)
                {
                    /* If the capacity has been resumed within the MINUTES_AFTER_RESUME_BEFORE_IDLE_CHECK time frame, 
                     * we will check the CPU metrics to see if it has been idle for the IDLE_MINUTES_BEFORE_PAUSE time frame
                     * We do this by directly querying the CPU metrics of the capacity for the last IDLE_MINUTES_BEFORE_PAUSE timeframe
                     * If max CPU is 0 over the whole timeframe, we will pause the capacity
                     */
                    try
                    {
                        var client = new MetricsQueryClient(new DefaultAzureCredential());

                        Response<MetricsQueryResult> results = client.QueryResource(
                            resourceId,
                            new[] { "cpu_metric" },
                            new MetricsQueryOptions
                            {
                                TimeRange = TimeSpan.FromMinutes(idleMinutesBeforePause),
                                Granularity = TimeSpan.FromMinutes(15),
                                Aggregations = { MetricAggregationType.Maximum }
                            });

                        foreach (MetricResult metric in results.Value.Metrics)
                        {
                            foreach (MetricTimeSeriesElement element in metric.TimeSeries)
                            {
                                double? max_cpu = element.Values.Max(v => v.Maximum);
                                log.LogInformation($"Max CPU in last {idleMinutesBeforePause} minutes: {max_cpu}%");
                                if (max_cpu == 0)
                                {
                                    try
                                    {
                                        log.LogInformation("Pausing capacity");
                                        capacity.Value.Suspend(Azure.WaitUntil.Completed);
                                        log.LogInformation("Capacity paused");
                                    } catch (Exception e)
                                    {
                                        log.LogError($"Error while pausing the capacity: {e.Message}");
                                        return;
                                    }
                                }
                            }
                        }
                    } catch (Exception e)
                    {
                        log.LogError($"Error getting metrics: {e.Message}");
                        return;
                    }
                    
                }
                
            }
            

            log.LogInformation($"Auto Pause function ran at: {DateTime.Now}");
        }
    }
}
