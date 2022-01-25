using Common;
using Confluent.Kafka;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;

namespace Service.Reporting
{
    class Program
    {
        private static Hashtable _mileageLog;
        private const int _serviceMileageMark = 500;

        static void Main(string[] args)
        {
            KafkaCommunicator communicator = KafkaCommunicator.Generate();
            List<string> topics = new List<string> { KafkaTopics.CentralLogging };
            _mileageLog = new Hashtable();

            for(short i = 1; i <= 5; ++i)
            {
                _mileageLog[i] = new VehicleReturnLog { VehicleId = i, ReturnMileage = 0, ServiceMileageMarker = _serviceMileageMark };
            }

            Console.WriteLine("Identity: Reporting (Consumer)");

            communicator.ConsumptionTopic(topics, "Reporting", Handler, true);
        }

        private static void Handler(ConsumeResult<Null, string> result)
        {
            VehicleReturnLog current = null;
            VehicleReturnLog incomingLog;
            string[] messageParts;

            try
            {
                messageParts = result.Message.Value.Split("|");

                if (messageParts.Length == 2)
                {
                    incomingLog = JsonSerializer.Deserialize<VehicleReturnLog>(messageParts[1]);

                    if (incomingLog != null)
                    {
                        Console.WriteLine($"Alert! - Vehicle return log detected. Performing analytics");
                        if (_mileageLog.ContainsKey(incomingLog.VehicleId))
                        {
                            current = (VehicleReturnLog)_mileageLog[incomingLog.VehicleId];

                            if(current.ReturnMileage > incomingLog.ReturnMileage)
                            {
                                NotifyAdmin("cars.admin@company.org", $"Vehicle {incomingLog.VehicleId} was returned with an invalid mileage log.");
                            }
                            else
                            {
                                current.ReturnMileage = incomingLog.ReturnMileage;
                                _mileageLog[incomingLog.VehicleId] = current;
                            }
                        }
                        else
                        {
                            Console.WriteLine("Unknown vehicle ID: " + incomingLog.VehicleId);
                        }

                        if (current != null && current.ReturnMileage > current.ServiceMileageMarker)
                        {
                            NotifyAdmin("cars.admin@company.org", $"Vehicle {incomingLog.VehicleId} is due for service. It will now be taken out of rotation.");

                            // Reset the service marker
                            current.ServiceMileageMarker = current.ReturnMileage + _serviceMileageMark;
                        }

                        PrintLog();
                    }
                }
            }
            catch (JsonException)
            {
                // This isn't a Vehicle return log
            }
        }
        
        private static void NotifyAdmin(string email, string message)
        {
            Console.WriteLine(JsonSerializer.Serialize(new { To = email, From = "system.notify@company.org", Body = message }));
        }

        private static void PrintLog()
        {
            VehicleReturnLog log;
            Console.WriteLine("\nPrinting Log");

            foreach(short id in _mileageLog.Keys)
            {
                log = (VehicleReturnLog)_mileageLog[id];
                Console.WriteLine($"VehicleID: {id}, Mileage: {log.ReturnMileage}, Next Service: {log.ServiceMileageMarker}");
            }
        }
    }
}
