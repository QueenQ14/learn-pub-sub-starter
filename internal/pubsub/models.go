package pubsub

type SimpleQueueType int
type AckType int

const (
	DurableQueue SimpleQueueType = iota
	TransientQueue
)

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)
