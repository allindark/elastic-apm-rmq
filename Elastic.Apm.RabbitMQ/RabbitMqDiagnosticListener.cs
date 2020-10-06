using Elastic.Apm.Api;
using Elastic.Apm.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Elastic.Apm.RabbitMQ
{
  internal class RabbitMqDiagnosticListener : IObserver<KeyValuePair<string, object>>
  {
    private readonly ConcurrentDictionary<Guid, IExecutionSegment> _processingQueries = new ConcurrentDictionary<Guid, IExecutionSegment>();
    private IApmAgent _ApmAgent;
    private readonly RabbitMqDiagnosticsOptions _Options;

    public RabbitMqDiagnosticListener(IApmAgent apmAgent, RabbitMqDiagnosticsOptions options)
    {
      _ApmAgent = apmAgent;
      _Options = options;
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
        case Constants.Events.SpanStart when value.Value is RabbitMqEvent<ApmSpanScopeParams> evt:
          HandleStartSpan(evt);
          break;
        case Constants.Events.SpanEnd when value.Value is RabbitMqDurationEvent<ApmSpanScopeParams> evt:
          HandleEndSpan(evt);
          break;
      }
    }

    private void HandleEndSpan(RabbitMqDurationEvent<ApmSpanScopeParams> evt)
    {
      try
      {
        if (!_processingQueries.TryRemove(evt.Params.Id, out var span)) return;
        if (evt.Params != null)
        {
          foreach (var item in evt.Params.Labels)
          {
            span.Labels.Add(item.Key, $"{item.Value}");
          }
        }
        TryFixThreadsCount(span, evt.Duration.TotalMilliseconds);
        span.Duration = evt.Duration.TotalMilliseconds;
        span.End();
      }
      catch { }
    }

    private void TryFixThreadsCount(IExecutionSegment span, double duration)
    {
      if (_Options?.LabelThreadsWhenDurationMs != null && _Options.LabelThreadsWhenDurationMs < duration)
      {
        ThreadPool.GetAvailableThreads(out int availableWT, out int availableIO);
        ThreadPool.GetMaxThreads(out int maxWT, out int maxIO);
        ThreadPool.GetMinThreads(out int minWT, out int minIO);
        span.Labels.Add("Threads.Min.Workers", $"{minWT}");
        span.Labels.Add("Threads.Min.IO", $"{minIO}");
        span.Labels.Add("Threads.Available.Workers", $"{availableWT}");
        span.Labels.Add("Threads.Available.IO", $"{availableIO}");
        span.Labels.Add("Threads.Uses.Workers", $"{maxWT - availableWT}");
        span.Labels.Add("Threads.Uses.IO", $"{maxIO - availableIO}");
      }
    }

    private void HandleStartSpan(RabbitMqEvent<ApmSpanScopeParams> evt)
    {
      try
      {
        var transaction = _ApmAgent.Tracer.CurrentTransaction;
        var currentExecutionSegment = _ApmAgent.Tracer.CurrentSpan ?? (IExecutionSegment)transaction;
        if (currentExecutionSegment == null)
          return;

        var span = currentExecutionSegment.StartSpan(
                      evt.Params.Command,
                      Constants.Type);

        if (!_processingQueries.TryAdd(evt.Params.Id, span)) return;
      }
      catch { }
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
        TryFixThreadsCount(span, evt.Duration.TotalMilliseconds);
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