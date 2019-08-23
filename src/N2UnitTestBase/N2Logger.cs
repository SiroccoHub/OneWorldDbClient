using Microsoft.Extensions.Logging;
using System;
using TestContext = NUnit.Framework.TestContext;

namespace N2UnitTestBase
{

    public class N2Logger<T> : ILogger<T>, IDisposable
    {
        public N2Logger()
        { }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            TestContext.WriteLine(state.ToString());
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return this;
        }

        public void Dispose()
        {
        }
    }
}
