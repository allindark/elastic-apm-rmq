using Elastic.Apm.Api;
using Elastic.Apm.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Elastic.Apm.RabbitMQ
{
  internal class RabbitMqDiagnosticListener : IObserver<KeyValuePair<string, object>>
  {
    private readonly ConcurrentDictionary<Guid, IExecutionSegment> _processingQueries = new ConcurrentDictionary<Guid, IExecutionSegment>();
    private IApmAgent _ApmAgent;
    private readonly IApmLogger _Logger;

    public RabbitMqDiagnosticListener(IApmAgent apmAgent)
    {
      _ApmAgent = apmAgent;
      _Logger = _ApmAgent.Logger;
    }

    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    public void OnNext(KeyValuePair<string, object> value)
    {
      switch (value.Key)
      {
        case Constants.Events.ReceiveStart when value.Value is RabbitMqEvent<RabbitMqHandleParams> evt:
          HandleStart(evt);
          break;
        case Constants.Events.ReceiveEnd when value.Value is RabbitMqDurationEvent<RabbitMqHandleParams> evt:
          HandleEnd(evt);
          break;
        case Constants.Events.ReceiveFail when value.Value is RabbitMqFailEvent<RabbitMqHandleParams> evt:
          HandleFail(evt);
          break;
      }
    }

    private void HandleStart(RabbitMqEvent<RabbitMqHandleParams> evt)
    {
      try
      {
        var prms = evt.Params;

        var transaction = _ApmAgent.Tracer.StartTransaction(prms.RoutingKey, ApiConstants.TypeExternal);

        if (!_processingQueries.TryAdd(prms.Id, transaction)) return;

        transaction.Context.Labels.Add(nameof(prms.RoutingKey), prms.RoutingKey);
        transaction.Context.Labels.Add(nameof(prms.ConsumerTag), prms.ConsumerTag);
        transaction.Context.Labels.Add(nameof(prms.DeliveryTag), $"{prms.DeliveryTag}");
        transaction.Context.Labels.Add(nameof(prms.Exchange), prms.Exchange);
        transaction.Context.Labels.Add(nameof(prms.Redelivered), $"{prms.ConsumerTag}");
        transaction.Context.Labels.Add(nameof(prms.Body), !prms.Body.IsEmpty ? System.Text.Encoding.UTF8.GetString(prms.Body.ToArray()) : string.Empty);
      }
      catch (Exception ex)
      {
        _Logger?.Log(LogLevel.Error, "Exception was thrown while handling 'command started event'", ex, null);
      }
    }

    private void HandleEnd(RabbitMqDurationEvent<RabbitMqHandleParams> evt)
    {
      try
      {
        if (!_processingQueries.TryRemove(evt.Params.Id, out var span)) return;
        span.Duration = evt.Duration.TotalMilliseconds;
        span.End();
      }
      catch (Exception ex)
      {
        _Logger?.Log(LogLevel.Error, "Exception was thrown while handling 'command succeeded event'", ex, null);
      }
    }

    private void HandleFail(RabbitMqFailEvent<RabbitMqHandleParams> evt)
    {
      try
      {
        if (!_processingQueries.TryRemove(evt.Params.Id, out var span)) return;
        span.Duration = evt.Duration.TotalMilliseconds;
        span.CaptureException(evt.Exception);
        span.End();
      }
      catch (Exception ex)
      {
        _Logger?.Log(LogLevel.Error, "Exception was thrown while handling 'command failed event'", ex, null);
      }
    }
  }
}