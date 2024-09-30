package watch

import "time"

type WatcherEventType string

const (
	Create WatcherEventType = "CREATE"
	Update WatcherEventType = "UPDATE"
	Delete WatcherEventType = "DELETE"
)

type WatcherEvent struct {
	EventType      WatcherEventType
	Key            []byte
	OldValue       []byte
	NewValue       []byte
	EventTimestamp time.Time
}

func NewCreateWatcherEvent(key, oldValue, newValue []byte) WatcherEvent {
	return makeNew(Create, key, oldValue, newValue)
}

func NewUpdateWatcherEvent(key, oldValue, newValue []byte) WatcherEvent {
	return makeNew(Update, key, oldValue, newValue)
}

func NewDeleteWatcherEvent(key, oldValue, newValue []byte) WatcherEvent {
	return makeNew(Delete, key, oldValue, newValue)
}

func makeNew(eventType WatcherEventType, key, oldValue, newValue []byte) WatcherEvent {
	return WatcherEvent{
		EventType:      eventType,
		Key:            key,
		OldValue:       oldValue,
		NewValue:       newValue,
		EventTimestamp: time.Now(),
	}
}
