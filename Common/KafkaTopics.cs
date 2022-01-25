namespace Common
{
    public class KafkaTopics
    {
        // allowed special characters for Kafka topic names ". _ -"
        public const string CentralLogging = "system.central_logging";
        public const string TaskExecution = "system.task_execution";
    }
}
