using RabbitMQ.Client;
using System;

namespace Elastic.Apm.RabbitMQ
{
  internal class RabbitMqEvent<T>
    where T : RabbitMqHandleParams
  {
    protected readonly T _Params;

    protected RabbitMqEvent(T @params)
    {
      _Params = @params;
    }

    public T Params => _Params;

    public static RabbitMqEvent<T> Success(T @params) => new RabbitMqEvent<T>(@params);
  }

  internal class RabbitMqDurationEvent<T> : RabbitMqEvent<T>
  where T : RabbitMqHandleParams
  {
    private readonly TimeSpan _Duration;

    protected RabbitMqDurationEvent(TimeSpan duration, T @params)
      : base(@params)
    {
      _Duration = duration;
    }

    public TimeSpan Duration => _Duration;

    public static RabbitMqDurationEvent<T> Success(TimeSpan duration, T @params) => new RabbitMqDurationEvent<T>(duration, @params);
  }

  internal class RabbitMqFailEvent<T> : RabbitMqDurationEvent<T>
    where T : RabbitMqHandleParams
  {
    private readonly Exception _Exception;
    private RabbitMqFailEvent(Exception ex, TimeSpan duration, T @params)
      : base(duration, @params)
    {
      _Exception = ex;
    }

    public Exception Exception => _Exception;
    public bool IsSuccess => Exception == null;

    public static RabbitMqFailEvent<T> Fail(Exception ex, TimeSpan duration, T @params) => new RabbitMqFailEvent<T>(ex, duration, @params);
  }

  internal class RabbitMqHandleParams
  {
    public RabbitMqHandleParams()
    {
      Id = Guid.NewGuid();
    }

    public Guid Id { get; }
    public string ConsumerTag { get; set; }
    public ulong DeliveryTag { get; set; }
    public bool Redelivered { get; set; }
    public string Exchange { get; set; }
    public string RoutingKey { get; set; }
    public IBasicProperties Properties { get; set; }
    public byte[] Body { get; set; }
    public string CommandName => "Receive";
  }
}