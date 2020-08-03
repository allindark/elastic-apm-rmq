using System;
using System.Collections.Generic;
using System.Text;

namespace Elastic.Apm.RabbitMQ
{
  static class Constants
  {
    internal const string DiagnosticName = "RabbitMQ.Client";
    internal const string Type = "rabbit_mq";

    internal const string HeaderKey = "elastic-apm-tracing-data";

    public static class Events
    {
      internal const string ReceiveStart = "ReceiveStart";
      internal const string ReceiveEnd = "ReceiveEnd";
      internal const string ReceiveFail = "ReceiveFail";
      internal const string SpanStart = "SpanStart";
      internal const string SpanEnd = "SpanEnd";
      internal const string PublishTracingHeader = "PublishTracingHeader";
    }
  }
}
