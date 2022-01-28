using System;
using System.Threading.Tasks;
using Common;
using Confluent.Kafka;
using System.Collections.Generic;

namespace Service.TaskExecution
{
    class Program
    {
        static void Main(string[] args)
        {
            KafkaCommunicator communicator = KafkaCommunicator.Generate();
            List<string> topics = new List<string> { KafkaTopics.TaskExecution };
            Action<ConsumeResult<Null, string>> handler = (r) => {
                Console.WriteLine($"Executing Task: {r.Message.Timestamp.UtcDateTime} - {r.Message.Value}");

                Task.Run(() => Task.Delay(15425)).Wait();

                Console.WriteLine($"Task Complete: {r.Message.Value}");
                communicator.SendMessageToTopic(KafkaTopics.CentralLogging, $"Task Complete|{r.Message.Value}");
            };

            Console.WriteLine("Identity: Task Execution Service (Consumer)");

            communicator.ConsumptionTopic(topics, "TaskService", handler);
        }
    }
}
