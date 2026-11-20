package pubsub

type SimpleQueueType int

const (
	DurableQueue SimpleQueueType = iota
	TransientQueue
)