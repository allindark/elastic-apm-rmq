using Elastic.Apm.DiagnosticSource;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Elastic.Apm.RabbitMQ
{
  public class RabbitMqDiagnosticsSubscriber : IDiagnosticsSubscriber
  {
    public IDisposable Subscribe(IApmAgent components)
    {
      var disposable = new CompositeDisposable();
      var initializer = new RabbitMqDiagnosticInitializer(components);

      disposable.Add(initializer);
      disposable.Add(DiagnosticListener
          .AllListeners
          .Subscribe(initializer));

      return disposable;
    }
  }
}
