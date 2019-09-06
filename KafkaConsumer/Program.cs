using Confluent.Kafka;
using System;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var logWriter = new LogWriter();

            var kafkaConfig = new ConsumerConfig()
            {
                BootstrapServers = "10.0.0.12:9092",
                EnableAutoCommit = false,
                SessionTimeoutMs = 20000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = "ContentConsumerTest"
            };

            var kafkaConsumer = new ConsumerBuilder<Ignore, string>(kafkaConfig)
                .SetErrorHandler((c, e) =>
                {
                    Console.WriteLine($"err[{e.Reason}]");
                })
                .SetLogHandler((c, e) =>
                {
                    Console.WriteLine($"info[{e.Message}]");
                })
                .Build();

            kafkaConsumer.Subscribe("Content.Case.DomainEventTopic");

            await Task.Run(async () =>
            {
                while (true)
                {
                    var message = kafkaConsumer.Consume();
                    if (message.Offset < 24614)
                    {
                        continue;
                    }

                    await logWriter.AddAsync(message.Value);
                }
            });

            Console.WriteLine("start consume");
            Console.ReadKey();
        }
    }
}