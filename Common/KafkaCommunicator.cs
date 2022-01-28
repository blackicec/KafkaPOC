using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;

namespace Common
{
    public class KafkaCommunicator
    {
        private static KafkaCommunicator _communicator;
        private KafkaCommunicator()
        {
            TopicInit(new List<string> { KafkaTopics.CentralLogging, KafkaTopics.TaskExecution });
        }

        public static KafkaCommunicator Generate()
        {
            if(_communicator == null)
            {
                _communicator = new KafkaCommunicator();
            }

            return _communicator;
        }

        public void SendMessageToTopic(string topic, string message, Action<DeliveryReport<Null, string>> handler = null)
        {
            using (var p = new ProducerBuilder<Null, string>(new ProducerConfig {
                BootstrapServers = Constants.BootstrapServers /*, SaslUsername = "", SaslPassword = ""*/ })
                .Build())
            {
                p.Produce(topic, new Message<Null, string> { Timestamp = Timestamp.Default, Value = message }, handler);

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
        }

        public void ConsumptionTopic(List<string> topics, string groupId, Action<ConsumeResult<Null, string>> handler)
        {
            using (IConsumer<Null, string> consumer =
                new ConsumerBuilder<Null, string>(new ConsumerConfig { GroupId = groupId, BootstrapServers = Constants.BootstrapServers }).Build()) { 
            
                consumer.Subscribe(topics);

                // Listen for incoming messages and print them out
                Console.WriteLine(groupId + ": Beginning Consumption");
                while (true)
                {
                    ConsumeResult<Null, string> result = consumer.Consume(5000);

                    if(result != null)
                    {
                        handler(result);
                    }
                }
            }
        }

        private void TopicInit(List<string> topics)
        {
            // Creation of topics
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Constants.BootstrapServers }).Build())
            {
                List<TopicSpecification> topicSpecifications = new List<TopicSpecification>();
                topics.ForEach(t => topicSpecifications.Add(new TopicSpecification { Name = t, NumPartitions = 1, ReplicationFactor = 1 }));

                try
                {
                    adminClient.CreateTopicsAsync(topicSpecifications).Wait();
                }
                catch (Exception) { /* Topic probably exists already*/ }
            }
        }
    }
}
