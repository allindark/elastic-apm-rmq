using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Elastic.Apm.RabbitMQ
{
  class RabbitMqDiagnosticInitializer : IObserver<DiagnosticListener>, IDisposable
  {
    private readonly IApmAgent _ApmAgent;
    private IDisposable _sourceSubscription;

    public RabbitMqDiagnosticInitializer(IApmAgent apmAgent)
    {
      _ApmAgent = apmAgent;
    }

    public void Dispose()
    {
      _sourceSubscription?.Dispose();
    }

    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    public void OnNext(DiagnosticListener value)
    {
      if (value.Name == Constants.DiagnosticName)
        _sourceSubscription = value.Subscribe(new RabbitMqDiagnosticListener(_ApmAgent));
    }
  }
}
