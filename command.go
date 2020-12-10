package casbin_hashicorp_raft

type Operation string

const (
	addOperation            Operation = "add"
	removeOperation                   = "remove"
	removeFilteredOperation           = "removeFiltered"
	clearOperation                    = "clear"
	updateOperation                   = "update"
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
