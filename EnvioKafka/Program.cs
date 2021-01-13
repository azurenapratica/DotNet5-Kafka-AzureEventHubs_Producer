using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace EnvioKafka
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
                .CreateLogger();
            logger.Information("Testando o envio de mensagens com Kafka + Azure Event Hubs");

            if (args.Length < 4)
            {
                Console.WriteLine(
                    "Informe ao menos 4 parametros: " +
                    "no primeiro o Host Name do Namespace do Azure Event Hubs (sera assumida a porta 9093), " +
                    "no segundo a string de conexão fornecida pelo Portal do Azure, " +
                    "no terceiro o Topic (Event Hub) que recebera as mensagens, " +
                    "ja no quarto em diante as mensagens a serem " +
                    "enviadas a um Topic/Event Hub...");
                return;
            }

            var bootstrapServers = $"{args[0]}:9093";
            var nomeTopic = args[2];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = "$ConnectionString",
                    SaslPassword = args[1]
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 3; i < args.Length; i++)
                    {
                        var result = await producer.ProduceAsync(
                            nomeTopic,
                            new () { Value = args[i] });

                        logger.Information(
                            $"Mensagem: {args[i]} | " +
                            $"Status: { result.Status.ToString()}");
                    }
                }

                logger.Information("Concluido o envio de mensagens");
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}
