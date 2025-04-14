using System;
using System.Threading;
using Confluent.Kafka;
using Newtonsoft.Json;

class Program
{
    static void Main(string[] args)
    {
        string bootstrapServers = "localhost:9092";  // Kafka server
        string topic = "random-data-topic";          // Kafka topic

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        using (var producer = new ProducerBuilder<string, string>(config).Build())
        {
            Random random = new Random();

            while (true)
            {
                // Generate random data
                var id = Guid.NewGuid().ToString();
                var value = random.Next(1, 100);
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                var data = new
                {
                    id = id,
                    value = value,
                    timestamp = timestamp
                };

                var message = new Message<string, string>
                {
                    Key = id,
                    Value = JsonConvert.SerializeObject(data)
                };

                try
                {
                    var result = producer.ProduceAsync(topic, message).Result;
                    Console.WriteLine($"Sent message: {message.Value} to partition: {result.Partition} at offset: {result.Offset}");
                }
                catch (ProduceException<string, string> e)
                {
                    Console.WriteLine($"Error producing message: {e.Message}");
                }

                // Sleep for a random interval
                Thread.Sleep(random.Next(500, 1000));
            }
        }
    }
}
