using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    public class LogWriter
    {
        private List<string> _contents = new List<string>();

        private AsyncLock _lock = new AsyncLock();

        public LogWriter()
        {
            Task.Run(async () => await WriteToFileAsync());
        }

        public async Task AddAsync(string content)
        {
            using (await _lock.LockAsync())
            {
                _contents.Add(content);
            }
        }

        private async Task WriteToFileAsync()
        {
            while (true)
            {
                if (_contents.Count > 0)
                {
                    using (await _lock.LockAsync())
                    {
                        await File.AppendAllLinesAsync("messages.txt", _contents);
                        _contents = new List<string>();
                    }
                }

                await Task.Delay(200);
            }
        }
    }
}