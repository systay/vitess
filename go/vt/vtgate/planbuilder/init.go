package planbuilder

import "vitess.io/vitess/go/vt/vtgate/semantics"

func init() {
	semantics.UseIDs = true
}
