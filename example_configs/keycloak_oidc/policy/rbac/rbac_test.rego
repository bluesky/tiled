package rbac_test

import data.rbac
import rego.v1

test_user_can_read_metadata if {
	rbac.allow with input as {"actions": ["read:metadata"]}
		with data.token as {"roles": ["user"]}
}

test_user_cannot_read_metadata if {
	not rbac.allow with input as {"actions": ["create"]}
		with data.token as {"roles": ["user"]}
}

test_user_permissions if {
	rbac.allow with input as {"actions": ["read:metadata", "write:metadata"]}
		with data.token as {"roles": ["user"]}
}
