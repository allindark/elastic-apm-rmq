using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Elastic.Apm.RabbitMQ
{
  public sealed class EventingBasicConsumerApm : AsyncEventingBasicConsumer
  {
    private static readonly System.Diagnostics.DiagnosticSource RabbitMqLogger =
            new DiagnosticListener(Constants.DiagnosticName);

    public EventingBasicConsumerApm(IModel model) : base(model)
    {
    }

    public override Task HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, byte[] body)
    {
      var prms = new RabbitMqHandleParams
      {
        ConsumerTag = consumerTag,
        DeliveryTag = deliveryTag,
        Redelivered = redelivered,
        Exchange = exchange,
        RoutingKey = routingKey,
        Properties = properties,
        Body = body
      };

      Task.Run(async () =>
      {
        Stopwatch sw = null;
        try
        {
          HandleStart(prms);
          sw = Stopwatch.StartNew();
          await base.HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body);
          sw.Stop();
          HandleEnd(sw.Elapsed, prms);
        }
        catch (Exception ex)
        {
          if (sw == null) return;

          sw.Stop();
          HandleFail(ex, sw.Elapsed, prms);
        }
      });
      return Task.CompletedTask;
    }

    private void HandleStart(RabbitMqHandleParams prms)
    {
      if (RabbitMqLogger.IsEnabled(Constants.Events.ReceiveStart))
        RabbitMqLogger.Write(Constants.Events.ReceiveStart, RabbitMqEvent<RabbitMqHandleParams>.Success(prms));
    }

    private void HandleEnd(TimeSpan duration, RabbitMqHandleParams prms)
    {
      if (RabbitMqLogger.IsEnabled(Constants.Events.ReceiveEnd))
        RabbitMqLogger.Write(Constants.Events.ReceiveEnd, RabbitMqDurationEvent<RabbitMqHandleParams>.Success(duration, prms));
    }

    private void HandleFail(Exception ex, TimeSpan duration, RabbitMqHandleParams prms)
    {
      if (RabbitMqLogger.IsEnabled(Constants.Events.ReceiveFail))
        RabbitMqLogger.Write(Constants.Events.ReceiveFail, RabbitMqFailEvent<RabbitMqHandleParams>.Fail(ex, duration, prms));
    }
  }
}
