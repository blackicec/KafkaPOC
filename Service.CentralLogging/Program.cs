using Common;
using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Service.CentralLogging
{
    class Program
    {
        static void Main(string[] args)
        {
            KafkaCommunicator communicator = KafkaCommunicator.Generate();
            List<string> topics = new List<string> { KafkaTopics.CentralLogging, KafkaTopics.TaskExecution };
            Action<ConsumeResult<Null, string>> handler = (r) => {
                if (r.Topic == KafkaTopics.CentralLogging)
                    Console.WriteLine($"General Logger: {r.Message.Timestamp.UtcDateTime} - {r.Message.Value}");
                else if (r.Topic == KafkaTopics.TaskExecution)
                    Console.WriteLine($"Task Execution Start: {r.Message.Timestamp.UtcDateTime} - {r.Message.Value}");
            };

            Console.WriteLine("Identity: Central Logging (Consumer)");

            communicator.ConsumptionTopic(topics, "CentralLogger", handler);
        }
    }
}
