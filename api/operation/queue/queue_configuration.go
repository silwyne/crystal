package queue

type QueueConfiguration struct {
	buffered     bool
	bufferLength int
}

const (
	DEFAULT_BUFFER_ENABLE = true
	DEFAULT_BUFFER_LENGTH = 128
)

func NewDefaultQueueConfiguration() QueueConfiguration {
	return QueueConfiguration{
		buffered:     DEFAULT_BUFFER_ENABLE,
		bufferLength: DEFAULT_BUFFER_LENGTH,
	}
}

func (q *QueueConfiguration) SetBuffered(buffered bool) {
	q.buffered = buffered
}

func (q *QueueConfiguration) SetBufferLength(bufferLength int) {
	q.bufferLength = bufferLength
}

func (q *QueueConfiguration) GetBuffered() bool {
	return q.buffered
}

func (q *QueueConfiguration) GetBufferLength() int {
	return q.bufferLength
}
