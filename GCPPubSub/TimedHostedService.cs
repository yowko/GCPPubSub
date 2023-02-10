using Google.Cloud.PubSub.V1;

namespace GCPPubSub;

public class TimedHostedService : BackgroundService
{
    private readonly ILogger<TimedHostedService> _logger;

    public TimedHostedService(ILogger<TimedHostedService> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("GcpPubSubHostedService is startting.");
        await PullMessages(stoppingToken);
    }


    private Task PullMessages(CancellationToken stoppingToken)
    {
        var projectId = "clean-skill-374402";
        var subscriptionId = "gkeupdate";
        var acknowledge = true;

        var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);
        var subscriber = SubscriberClient.Create(subscriptionName);

        return subscriber.StartAsync((PubsubMessage message, CancellationToken cancel) =>
        {
            var result = System.Text.Encoding.UTF8.GetString(message.Data.ToArray());

            // 這邊可以呼叫其他服務
            Console.WriteLine(
                $"[{DateTimeOffset.Now}]{result}@{message.PublishTime.ToDateTimeOffset()} from {message.Attributes["cluster_name"]}");

            // 下面是用來處理呼叫其他服務後的結果，再決定是否要 ack 刪除通知
            return Task.FromResult(acknowledge ? SubscriberClient.Reply.Ack : SubscriberClient.Reply.Nack);
        });
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Consume Scoped Service Hosted Service is stopping.");

        await base.StopAsync(stoppingToken);
    }
}