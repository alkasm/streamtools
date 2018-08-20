## Example Usage

```python
from streamtools import StreamsClient, StreamsInteractor, StreamsReader
import random

# start the Redis Streams client
client = StreamsClient('localhost')

# write some random stuff to two streams
writer = StreamsInteractor(client)
for i in range(100):
    x = random.randint(0, 100)
    writer.xadd('ex-stream-1', **{'x': x, 'x+5': x + 5})
    writer.xadd('ex-stream-2', **{'x': x, 'x+7': x + 7})

# iterate through all values written, starting with the earliest values
streams = {'ex-stream-1': 0, 'ex-stream-2': 0}
reader = StreamsReader(client, streams, continuous=False)
for data in reader:
    print(data)
```

> StreamData(id='1534799601773-0', key='ex-stream-1', data={'x+5': '26', 'x': '21'})  
> StreamData(id='1534799601773-0', key='ex-stream-2', data={'x+7': '28', 'x': '21'})  
> StreamData(id='1534799601773-1', key='ex-stream-1', data={'x+5': '34', 'x': '29'})  
> StreamData(id='1534799601773-1', key='ex-stream-2', data={'x+7': '36', 'x': '29'})  
> StreamData(id='1534799601774-0', key='ex-stream-1', data={'x+5': '41', 'x': '36'})  
> StreamData(id='1534799601774-0', key='ex-stream-2', data={'x+7': '43', 'x': '36'})  
> ...  
