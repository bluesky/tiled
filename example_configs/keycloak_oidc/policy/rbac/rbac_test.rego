package rbac_test

import data.rbac

admin_tag := "facility_admin"

users := {
	"alice": {"tags": ["beamline_x_user"]},
	"bob": {"tags": ["beamline_y_user"]},
	"cara": {"tags": [admin_tag]},
	"admin": {"tags": [admin_tag, "beamline_x_user"]},
}

test_allowed_to_every_tag_if_admin if {
	rbac.allow with input as {"tags": ["public"]}
		with data.token as {"name": "admin"}
		with rbac.users as users
}

test_not_allowed_to_add_invalid_tags if {
	not rbac.allow with input as {"tags": ["z_beamline"]}
		with data.token as {"name": "admin"}
		with rbac.users as users
}

test_user_allowed_to_add_user_tags if {
	rbac.allow with input as {"tags": ["beamline_y_user"]}
		with data.token as {"name": "bob"}
		with rbac.users as users
}

test_user_not_allowed_to_add_invalid_tags if {
	not rbac.allow with input as {"tags": ["beamline_x_user"]}
		with data.token as {"name": "bob"}
		with rbac.users as users
}

test_user_is_admin if {
	rbac.is_admin with data.token as {"name": "admin"}
		with rbac.users as users
}

test_user_is_not_admin if {
	not rbac.is_admin with data.token as {"name": "alice"}
		with rbac.users as users
}

test_admin_has_all_tags if {
	rbac.tags == {"facility_admin", "beamline_y_user", "beamline_x_user", "public"} with data.token as {"name": "admin"}
		with rbac.users as users
}

test_beamline_user_has_only_beamline_tags if {
	rbac.tags == {"beamline_x_user", "public"} with data.token as {"name": "alice"}
		with rbac.users as users
}

test_allowed_tags if {
	rbac.allowed_tags == {"beamline_x_user"} with input as {"tags": ["beamline_x_user"]}
		with data.token as {"name": "admin"}
		with rbac.users as users
}

test_allowed_tags_for_public_tag if {
	rbac.allowed_tags == {"public"} with input as {"tags": ["public"]}
		with data.token as {"name": "alice"}
		with rbac.users as users
}

test_allowed_scopes_for_admin if {
	rbac.scopes == {
		"read:data",
		"read:metadata",
		"write:data",
		"write:metadata",
		"create:node",
		"register",
		"delete:node",
		"delete:revision",
	} with input as {"tags": ["facility_admin"]}
		with data.token as {"name": "admin"}
		with rbac.users as users
}

test_allowed_scopes_for_admin_for_any_resource if {
	rbac.scopes == {
		"read:data",
		"read:metadata",
		"write:data",
		"write:metadata",
		"create:node",
		"register",
		"delete:node",
		"delete:revision",
	} with input as {"tags": ["beamline_x_user"]}
		with data.token as {"name": "admin"}
		with rbac.users as users
}

test_allowed_scopes_for_unauthorized_user if {
	count(rbac.scopes) == 0 with input as {"tags": ["facility_admin"]}
		with data.token as {"name": "alice"}
		with rbac.users as users
}

test_allowed_scopes_for_user if {
	rbac.scopes == {
		"read:data",
		"read:metadata",
		"write:data",
		"write:metadata",
	} with input as {"tags": ["beamline_x_user"]}
		with data.token as {"name": "alice"}
		with rbac.users as users
}
