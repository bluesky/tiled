package rbac

import rego.v1

import data.token

# role-permissions assignments
role_permissions := {
	"user": [
		{"action": "read:metadata"},
		{"action": "read:data"},
		{"action": "write:metadata"},
		{"action": "write:data"},
	],
	"service": [{"action": "metrics"}],
}

default allow := false

# METADATA
# description: Check permission for each user
# entrypoint: true
allow if token.name == "admin"

allow if token.name == input.attribute.created_by

allow if input.attribute.created_by == "allow_all"

allow if {
	every action in input.actions {
		some r in token.roles
		some p in role_permissions[r]
		p == {"action": action}
	}
}
