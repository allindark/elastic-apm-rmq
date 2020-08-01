using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Elastic.Apm.RabbitMQ
{
  public static class IModelExtensions
  {
    private static readonly System.Diagnostics.DiagnosticSource RabbitMqLogger =
            new DiagnosticListener(Constants.DiagnosticName);

    public static void BasicPublishApm(this IModel channel, string exchange, string routingKey, IBasicProperties basicProperties, byte[] body)
    {
      if (basicProperties == null)
        basicProperties = channel.CreateBasicProperties();

      PublishTracingHeader(basicProperties);

      channel.BasicPublish(exchange: exchange, routingKey: routingKey, basicProperties: basicProperties, body: body);
    }

    private static void PublishTracingHeader(IBasicProperties basicProperties)
    {
      if (basicProperties != null)
        RabbitMqLogger.Write(Constants.Events.PublishTracingHeader, RabbitMqEvent<IBasicProperties>.Success(basicProperties));
    }
  }
}
