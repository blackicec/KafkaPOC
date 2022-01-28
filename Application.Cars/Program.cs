using Common;
using Confluent.Kafka;
using System;
using System.Text.Json;

namespace Application.Cars
{
    class Program
    {
        static void Main(string[] args)
        {
            const string applicationName = "Cars";
            KafkaCommunicator communicator = KafkaCommunicator.Generate();
            Action<DeliveryReport<Null, string>> handler =
                (r) => Console.WriteLine(!r.Error.IsError ? $"Message successfully sent to Kafka topic '{args[0]}'." : $"There was an error sending the {args[0]} topic.");

            if (args.Length < 1)
            {
                Console.WriteLine("Error: Expected command and value parameters. Ex: [Program].exe [log|task] {message if task command given}");

                return;
            }

            switch (args[0].ToLower())
            {
                case "log":
                    VehicleReturnLog log = GetReturnDetails();
                    string message = JsonSerializer.Serialize(log);

                    communicator.SendMessageToTopic(KafkaTopics.CentralLogging, $"{applicationName}|{message}", handler);
                    break;
                case "task":
                    communicator.SendMessageToTopic(KafkaTopics.TaskExecution, $"{applicationName}|{args[1]}", handler);
                    break;
                default:
                    Console.WriteLine("Error: Unknown command was provided. Expected [log|task].");
                    break;
            }

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }

        private static VehicleReturnLog GetReturnDetails()
        {
            VehicleReturnLog log = new VehicleReturnLog();

            Console.Write("Vehicle ID: ");
            log.VehicleId = short.Parse(Console.ReadLine());

            Console.Write("Return Mileage: ");
            log.ReturnMileage = double.Parse(Console.ReadLine());

            return log;
        }
    }
}
