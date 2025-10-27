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

test_admin_access if {
	rbac.allow with input as {"actions": ["read:metadata", "write:metadata"]}
		with data.token as {"name": "admin"}
}

test_created_by_same_person if {
	rbac.allow with input as {"attribute": {"created_by": "alice"}}
		with data.token as {"name": "alice"}
}

test_created_by_different_person if {
	not rbac.allow with input as {"attribute": {"created_by": "alice"}}
		with data.token as {"name": "bob"}
}

test_created_by_for_all if {
	rbac.allow with input as {"attribute": {"created_by": "allow_all"}}
		with data.token as {"name": "bob"}
}

test_created_by_for_all_new if {
	rbac.allow with input as {"attribute": {"resource_type": "unknown"}}
		with data.token as {"name": "bob"}
}


