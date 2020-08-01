using Elastic.Apm.Api;
using Elastic.Apm.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Elastic.Apm.RabbitMQ
{
  internal class RabbitMqDiagnosticListener : IObserver<KeyValuePair<string, object>>
  {
    private readonly ConcurrentDictionary<Guid, IExecutionSegment> _processingQueries = new ConcurrentDictionary<Guid, IExecutionSegment>();
    private IApmAgent _ApmAgent;

    public RabbitMqDiagnosticListener(IApmAgent apmAgent)
    {
      _ApmAgent = apmAgent;
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
        case Constants.Events.PublishTracingHeader when value.Value is RabbitMqEvent<IBasicProperties> evt:
          HandleTraceHeader(evt);
          break;
      }
    }

    private void HandleTraceHeader(RabbitMqEvent<IBasicProperties> evt)
    {
      try
      {
        var transaction = _ApmAgent.Tracer.CurrentTransaction;
        if (transaction == null)
        {
          return;
        }

        evt.Params.Headers.Add(Constants.HeaderKey, Encoding.UTF8.GetBytes(transaction.OutgoingDistributedTracingData?.SerializeToString()));
      }
      catch
      {
      }
    }

    private void HandleStart(RabbitMqEvent<RabbitMqHandleParams> evt)
    {
      try
      {
        var prms = evt.Params;
        DistributedTracingData tracingData = null;
        if (prms.Properties != null && prms.Properties.Headers.TryGetValue(Constants.HeaderKey, out object tracingDataBlob) && tracingDataBlob is byte[])
        {
          tracingData = DistributedTracingData.TryDeserializeFromString(Encoding.UTF8.GetString((byte[])tracingDataBlob));
        }

        var transaction = _ApmAgent.Tracer.StartTransaction(prms.RoutingKey, Constants.Type, tracingData);

        if (!_processingQueries.TryAdd(prms.Id, transaction)) return;

        transaction.Context.Labels.Add(nameof(prms.RoutingKey), prms.RoutingKey);
        transaction.Context.Labels.Add(nameof(prms.ConsumerTag), prms.ConsumerTag);
        transaction.Context.Labels.Add(nameof(prms.DeliveryTag), $"{prms.DeliveryTag}");
        transaction.Context.Labels.Add(nameof(prms.Exchange), prms.Exchange);
        transaction.Context.Labels.Add(nameof(prms.Redelivered), $"{prms.ConsumerTag}");
        transaction.Context.Labels.Add(nameof(prms.Body), prms.Body != null ? System.Text.Encoding.UTF8.GetString(prms.Body) : string.Empty);
      }
      catch
      {
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
      catch { }
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
      catch { }
    }
  }
}