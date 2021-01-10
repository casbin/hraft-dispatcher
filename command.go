package casbin_hraft_dispatcher

import "encoding/json"

type Operation string

const (
	AddOperation            Operation = "add"
	RemoveOperation                   = "remove"
	RemoveFilteredOperation           = "removeFiltered"
	ClearOperation                    = "clear"
	UpdateOperation                   = "update"
)

// Command represents an instruction to change the state of the engine
type Command struct {
	Operation Operation `json:"operation"`
	Sec       string    `json:"sec"`

	Ptype string `json:"ptype"`
	// These fields will be used to add or delete rules
	Rules [][]string `json:"rules"`
	// These fields will be used to field rules
	FieldIndex  int      `json:"fieldIndex"`
	FieldValues []string `json:"fieldValues"`
	// These fields will be used to update rules
	NewRule []string `json:"newRule"`
	OldRule []string `json:"oldRule"`
}

func (c *Command) ToBytes() ([]byte, error) {
	return json.Marshal(c)
}
