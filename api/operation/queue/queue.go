package queue

import "github.com/Silwyne/crystal/api/row"

func MakeQueue(queueConfig *QueueConfiguration) chan row.Row {
	var channel chan row.Row
	if queueConfig.GetBuffered() {
		buffer_length := queueConfig.GetBufferLength()
		if buffer_length <= 0 {
			panic("BufferLength can not be equal or less than 0")
		}
		channel = make(chan row.Row, buffer_length)
	} else {
		channel = make(chan row.Row)
	}
	return channel
}
