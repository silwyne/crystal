# Featutre TODO list

## Explanation 
Here I list features that are necessary for having a stream processing framework.
These features make the user feel like this is a real tool while it actually isn't.
My hope of having a Stream processing framework in golang is fading, while I know other engineers in big companies are working on the same idea.

## List of Features
- Unified scalable Queue \
Each transformation layer gives a list of channels as result that are  input channels for the next transformation layer.
When the parallelism of these two transformation layers are different the number of input channels do not match the number of parallel instances.
This means some kind of Algorithm is needed to distribute the current channels items into these parallel instances.
But I do not feel enough efficiency in this.
Somthing like a kafka topic let you add a new consumer or even remove while consuming data.
From **Unified Scalable Queue** I mean that!

- Automatic Operator Chaining \
For example if the next operator is faster than the current one.
We can run them together in the same transformation layer.
Which leads to less number of goroutines, less cpu consumption, less overhead, less latency.

- Http Monitoring Server \
A server that holds important metrics and visualizes them.
    - number of rows processed
    - number of input rows in the queue
    - latency
    - job graph
    - parallelism
