using System;
using System.Collections.Generic;
using System.Text;

namespace Elastic.Apm.RabbitMQ
{
  public class RabbitMqDiagnosticsOptions
  {
    public RabbitMqDiagnosticsOptions()
    {
    }

    /// <summary>
    /// Add thread pool labels when duration grater then...
    /// </summary>
    public double? LabelThreadsWhenDurationMs { get; set; }
    
    internal static RabbitMqDiagnosticsOptions Default()
    {
      return new RabbitMqDiagnosticsOptions();
    }
  }
}
