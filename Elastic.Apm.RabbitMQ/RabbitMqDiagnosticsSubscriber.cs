using Elastic.Apm.DiagnosticSource;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Elastic.Apm.RabbitMQ
{
  public class RabbitMqDiagnosticsSubscriber : IDiagnosticsSubscriber
  {
    private readonly RabbitMqDiagnosticsOptions _Options;

    public RabbitMqDiagnosticsSubscriber(RabbitMqDiagnosticsOptions options = null)
    {
      _Options = options ?? RabbitMqDiagnosticsOptions.Default();
    }

    public IDisposable Subscribe(IApmAgent components)
    {
      var disposable = new CompositeDisposable();
      var initializer = new RabbitMqDiagnosticInitializer(components, _Options);

      disposable.Add(initializer);
      disposable.Add(DiagnosticListener
          .AllListeners
          .Subscribe(initializer));

      return disposable;
    }
  }
}
