package events

// MetadataChange is an event that describes changes to topology metadata
type MetadataChange struct {
	KeyspaceName string
	Key          string
	Status       string
}
