using Elastic.Apm.Api;
using Elastic.Apm.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Elastic.Apm.RabbitMQ
{
  internal class RabbitMqDiagnosticListener : IObserver<KeyValuePair<string, object>>
  {
    private readonly ConcurrentDictionary<Guid, ISpan> _processingQueries = new ConcurrentDictionary<Guid, ISpan>();
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

        var transaction = _ApmAgent.Tracer.CurrentTransaction;
        var currentExecutionSegment = _ApmAgent.Tracer.CurrentSpan ?? (IExecutionSegment)transaction;
        if (currentExecutionSegment == null)
          currentExecutionSegment = _ApmAgent.Tracer.StartTransaction(prms.CommandName, ApiConstants.TypeExternal);

        var span = currentExecutionSegment.StartSpan(
            prms.CommandName,
            ApiConstants.TypeExternal,
            "mongo");

        if (!_processingQueries.TryAdd(prms.Id, span)) return;

        span.Action = ApiConstants.ActionExec;
        span.Context.Labels.Add(nameof(prms.ConsumerTag), prms.ConsumerTag);
        span.Context.Labels.Add(nameof(prms.DeliveryTag), $"{prms.DeliveryTag}");
        span.Context.Labels.Add(nameof(prms.Exchange), prms.Exchange);
        span.Context.Labels.Add(nameof(prms.Redelivered), $"{prms.ConsumerTag}");
        span.Context.Labels.Add(nameof(prms.Body), prms.Body?.Length > 0 ? System.Text.Encoding.UTF8.GetString(prms.Body) : string.Empty);
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