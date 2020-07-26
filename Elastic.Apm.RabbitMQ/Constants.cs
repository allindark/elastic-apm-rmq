using System;
using System.Collections.Generic;
using System.Text;

namespace Elastic.Apm.RabbitMQ
{
  static class Constants
  {
    internal const string DiagnosticName = "RabbitMQ.Client";

    public static class Events
    {
      internal const string ReceiveStart = "ReceiveStart";
      internal const string ReceiveEnd = "ReceiveEnd";
      internal const string ReceiveFail = "ReceiveFail";
    }
  }
}
