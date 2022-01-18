using Common;
using Confluent.Kafka;
using System;

namespace Service.CentralLogging
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Identity: Central Logging (Consumer)");
            Consumption();
        }

        
    }
}
