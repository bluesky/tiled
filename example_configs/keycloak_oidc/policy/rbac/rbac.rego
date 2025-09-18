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
	"admin": [
		{"action": "read:metadata"},
		{"action": "read:data"},
		{"action": "write:metadata"},
		{"action": "write:data"},
		{"action": "create"},
		{"action": "register"},
	],
	"service": [{"action": "metrics"}],
}

default allow := false

# METADATA
# description: Check permission for each user
# entrypoint: true
allow if {
	every action in input.actions {
		some r in token.roles
		some p in role_permissions[r]
		p == {"action": action}
	}
}
