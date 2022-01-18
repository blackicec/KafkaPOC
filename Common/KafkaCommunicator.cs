using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public class KafkaCommunicator
    {
        public KafkaCommunicator()
        {
        }

        public void SendMessageToTopic(string topic, Action<DeliveryReport<Null, string>> handler = null)
        {
            string message = string.Empty;

            // Creation of topics
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Constants.BootstrapServers }).Build())
            {
                try
                {
                    adminClient.CreateTopicsAsync(
                        new TopicSpecification[]
                        {
                            new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }
                        }).Wait();
                }
                catch (Exception) { /* Topic probably exists already*/ }
            }

            using (var p = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = Constants.BootstrapServers, SaslUsername = "", SaslPassword = "" }).Build())
            {
                p.Produce(topic, new Message<Null, string> { Timestamp = Timestamp.Default, Value = message }, handler);

                Console.WriteLine("Production Complete. Goodbye for now...");

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
        }

        private static void ConsumptionTopic(List<string> topics, string groupId, Action<ConsumeResult<Null, string>> handler)
        {
            string topic = string.Empty;

            using (IConsumer<Null, string> consumer =
                new ConsumerBuilder<Null, string>(new ConsumerConfig { GroupId = groupId, BootstrapServers = Constants.BootstrapServers }).Build())
            {
                consumer.Subscribe(topics);

                // Listen for incoming messages and print them out
                while (true)
                {
                    ConsumeResult<Null, string> result = consumer.Consume();

                    handler(result);

                    if (result != null)
                    {
                        Console.WriteLine($"Logged entry to DB - Payload: {result.Message.Value}, Time: {result.Message.Timestamp.UtcDateTime}");
                    }
                }
            }
        }
    }
}
