using System;
using System.Collections.Generic;
using System.Text;

namespace Elastic.Apm.RabbitMQ
{
  class ApmSpanScopeParams
  {
    private readonly Dictionary<string, string> _Labels = new Dictionary<string, string>();
    public ApmSpanScopeParams(string command)
    {
      Id = Guid.NewGuid();
      Command = command;
    }

    public Guid Id { get; }
    public string Command { get; }
    public IReadOnlyCollection<KeyValuePair<string, string>> Labels { get { return _Labels; } }
    public void AddLabel(string key, string value) => _Labels.Add(key, value);
  }
  public interface IApmSpanScope : IDisposable
  {
    void AddLabel(string key, string value);
  }
}
