package ring

// PublishResult is the admission result returned by TryPublish.
type PublishResult uint8

const (
	PublishAccepted PublishResult = iota
	PublishFull
	PublishClosed
	PublishInvalid
)

func (r PublishResult) String() string {
	switch r {
	case PublishAccepted:
		return "accepted"
	case PublishFull:
		return "queue_full"
	case PublishClosed:
		return "closed"
	case PublishInvalid:
		return "invalid"
	default:
		return "unknown"
	}
}
