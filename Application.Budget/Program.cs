using Common;
using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Application.Budget
{
    class Program
    {
        static void Main(string[] args)
        {
            const string applicationName = "Budget";
            KafkaCommunicator communicator = KafkaCommunicator.Generate();
            Action<DeliveryReport<Null, string>> handler =
                (r) => Console.WriteLine(!r.Error.IsError ? $"Message successfully sent to Kafka topic '{args[0]}'." : $"There was an error sending the {args[0]} topic.");

            if (args.Length < 2)
            {
                Console.WriteLine("Error: Expected command and value parameters. Ex: [Program].exe [log|task] \"{Task name or log message}\"");

                return;
            }

            switch (args[0].ToLower())
            {
                case "log":
                    communicator.SendMessageToTopic(KafkaTopics.CentralLogging, $"{applicationName}|{args[1]}", handler);
                    break;
                case "task":
                    communicator.SendMessageToTopic(KafkaTopics.TaskExecution, $"{applicationName}|{args[1]}", handler);
                    break;
                default:
                    Console.WriteLine("Error: Unknown command was provided. Expected [log|task].");
                    break;
            }
        }
    }
}
