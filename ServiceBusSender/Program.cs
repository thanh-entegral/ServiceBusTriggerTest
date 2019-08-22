using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace ServiceBusSenderReceiver
{
    class Program
    {
        static StreamWriter logger = new StreamWriter(new FileStream("simulator_log.txt", FileMode.CreateNew));
        const string ServiceBusConnectionString = "Endpoint=sb://testingdurablefnsb.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fCoXMoWab4vtT33Y3wFELIoeAdghNTOJxBQItfbIkGQ=";
        const string TopicName = "testingdurablefntopic";
        const string SubscriptionName = "testingdurablefnsubscription";
        const int default_numberOfMessages = 6000;
        const int default_numberOfCars = 50;
        const int default_tps = 5;

        static ITopicClient topicClient;
        static void Main(string[] args)
        {
            if (File.Exists("simulator_log.txt")) File.Delete("simulator_log.txt");
            MainAsync(args).GetAwaiter().GetResult();
            logger.Flush();
        }

        static async Task SendMessagesAsync(int car_id, int numberOfMessagesToSend, int tps)
        {
            List<Task> sendTaskList = new List<Task>();
            int delay = 1000 / tps;
            for (var i = 0; i < numberOfMessagesToSend; i++)
            {
                try
                {
                    DateTime start = DateTime.Now;
                    Guid msg_id = Guid.NewGuid();
                    string messageBody = $"{{car_id: {car_id}, message_id: \"{msg_id}\", TimeStamp: \"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}\" }}";
                    var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                    message.SessionId = car_id.ToString();
                    // Write the body of the message to the console
                    Console.WriteLine($"[{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}]: Sending message: {messageBody}");

                    lock (logger)
                    {
                        logger.WriteLine($"{DateTime.UtcNow.ToString("HH:mm:ss.fff")};{msg_id};{car_id}");
                    }

                    // Send the message to the topic
                    sendTaskList.Add(topicClient.SendAsync(message));
                    var time_spent_total = (int)(DateTime.Now - start).TotalMilliseconds;
                    if (time_spent_total < delay)
                    {
                        Thread.Sleep(delay - time_spent_total);
                    }
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
                }
            }
            await Task.WhenAll(sendTaskList);
        }
        static Dictionary<string, int> ParseArguments(string[] args)
        {
            Dictionary<string, int> result = new Dictionary<string, int>();
            for (int i = 0; i < args.Length-1; i += 2)
            {
                string currentArg = args[i];
                int currentVal = int.Parse(args[i + 1]);
                currentArg = currentArg.Substring(currentArg.LastIndexOf('-')+1);
                result.Add(currentArg, currentVal);
            }
            return result;
        }
        static async Task MainAsync(string[] args)
        {
            var arguments = ParseArguments(args);
            int numberOfMessages = default_numberOfMessages;
            int numberOfCars = default_numberOfCars;
            int tps = default_tps;
            if (arguments.ContainsKey("cars"))
            {
                numberOfCars = arguments["cars"];
            }
            if (arguments.ContainsKey("messages"))
            {
                numberOfMessages = arguments["messages"];
            }
            if (arguments.ContainsKey("tps"))
            {
                tps = arguments["tps"];
            }
            topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

            List<Task> car_tasks = new List<Task>();
            // Send Messages
            for (int c = 0; c < numberOfCars; c++)
            {
                car_tasks.Add(Task.Factory.StartNew(async o => {
                    var i1 = (int)o;
                    await SendMessagesAsync(i1, numberOfMessages, tps);
                }, c));
            }
            await Task.WhenAll(car_tasks);
            Console.ReadKey();
            Console.WriteLine("Finished running for all cars");
            await topicClient.CloseAsync();
        }
    }
}
