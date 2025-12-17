package rbac

import data.token

public_tag := {"public"}

admin_tag := "facility_admin"
tag_permissions := {
	"beamline_y_user": [
		"read:data",
		"read:metadata",
	],
	admin_tag: [
		"read:data",
		"read:metadata",
		"write:data",
		"write:metadata",
		"create:node",
		"register",
		"delete:revision",
		"delete:node",
	],
	"beamline_x_user": [
		"read:data",
		"read:metadata",
		"write:data",
		"write:metadata",
	],
	"public": [
		"read:data",
		"read:metadata",
	],
}

users := {
	"alice": {"tags": ["beamline_x_user"]},
	"bob": {"tags": ["beamline_y_user"]},
	"cara": {"tags": [admin_tag]},
	"admin": {"tags": [admin_tag, "beamline_x_user"]},
}

default is_admin := false

is_admin if {
	admin_tag in users[token.name].tags
}

tags contains tag if {
	some tag in users[token.name].tags
}

tags contains tag if {
	some tag in public_tag
}

tags contains tag if {
	is_admin
	some tag in object.keys(tag_permissions)
}

input_tags contains tag if some tag in input.tags
allowed_tags := tags & input_tags

scopes contains p if {
	some tag in allowed_tags
	some p in tag_permissions[tag]
}

scopes contains p if {
	is_admin
	some p in tag_permissions[admin_tag]
}

tag_valid if {
	every tag in input_tags {
		tag in object.keys(tag_permissions)
	}
}

user_tags contains tag if some tag in users[token.name].tags

extra_tags := input_tags - user_tags

default allow := false

allow if {
	tag_valid
	count(extra_tags) == 0
}

allow if {
	tag_valid
	is_admin
}
