using Google.Cloud.PubSub.V1;

namespace GCPPubSub;

public class TimedHostedService : IHostedService, IDisposable
{
    private int executionCount = 0;
    private readonly ILogger<TimedHostedService> _logger;
    private Timer? _timer = null;

    public TimedHostedService(ILogger<TimedHostedService> logger)
    {
        _logger = logger;
    }

    public Task StartAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Timed Hosted Service running.");
        
        _timer = new Timer(PullMessages, null, TimeSpan.Zero, TimeSpan.FromSeconds(5));
        return Task.CompletedTask;
    }

    private void PullMessages(object? _object)
    {
        var projectId = "clean-skill-374402";
        var subscriptionId = "gkeupdate";
        var acknowledge = true;

        var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);
        var subscriber = SubscriberClient.Create(subscriptionName);

        subscriber.StartAsync((PubsubMessage message, CancellationToken cancel) =>
        {
            var result = System.Text.Encoding.UTF8.GetString(message.Data.ToArray());
            Console.WriteLine(
                $"[{DateTimeOffset.Now}]{result}@{message.PublishTime.ToDateTimeOffset()} from {message.Attributes["cluster_name"]}");

            return Task.FromResult(acknowledge ? SubscriberClient.Reply.Ack : SubscriberClient.Reply.Nack);
        }).GetAwaiter().GetResult();
        
    }
    
    public Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Timed Hosted Service is stopping.");

        _timer?.Change(Timeout.Infinite, 0);

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }
}