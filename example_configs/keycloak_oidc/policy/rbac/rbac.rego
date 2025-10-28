package rbac

import rego.v1

import data.token

import data.role_permissions
import data.resource
import data.users

# # role-permissions assignments
# role_permissions := {
# 	"user": [
# 		{"action": "read:metadata"},
# 		{"action": "read:data"},
# 		{"action": "write:metadata"},
# 		{"action": "write:data"},
# 	],
# 	"service": [{"action": "metrics"}],
# }

default allow := false

# # METADATA
# # description: Check permission for each user
# # entrypoint: true
# allow if token.name == "admin"

# allow if token.name == input.attribute.created_by

# allow if input.attribute.created_by == "allow_all"

# allow if {
# 	every action in input.actions {
# 		some r in token.roles
# 		some p in role_permissions[r]
# 		p == {"action": action}
# 	}
# }


user := token.name

resource_type := input.attribute.resource_type

resource_config := resource[resource_type] if resource_type in resource else := resource["UnTagged_document"] 

# include if {
# 	input.attribute.resource_type=="A"
# }

# allow if input.attribute.resource_type=="A"

allow if resource_config["allowed_roles"] == ["public","facility_admin"]




	# Role-based scopes from roles that are allowed for this resource
	role_scopes := {s |
		role := user_roles[_]
		role in res.allowed_roles
		s := role_permissions[role][_]
	}

	# Optionally include public access for all users
	public_scopes := {s |
		"public" in res.allowed_roles
		s := role_permissions.public[_]
	}
