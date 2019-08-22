using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Threading.Tasks;



namespace SegmentDurationTracker
{
    public class Car
    {
        public int car_id { get; set; }

        public string message_id { get; set; }
    }
    public static class MessageReceiver
    {
        [FunctionName("ServiceBusReceiver")]
        public static async Task RunAsync([ServiceBusTrigger("testingdurablefntopic", "testingdurablefnsubscription",
            Connection = "connection",IsSessionsEnabled = false)]
            string mySbMsg,
            ILogger log)
        {
            Car car = JsonConvert.DeserializeObject<Car>(mySbMsg);
            var car_id = car.car_id.ToString();

            log.LogInformation($"{DateTime.UtcNow.ToString("HH:mm:ss.fff")};{car.message_id};{car_id}");
        }
    }
}
