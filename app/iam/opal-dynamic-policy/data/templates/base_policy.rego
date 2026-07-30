package authz.base

default allow = false

allow {
    input.tenant_id == input.user.tenant_id
    input.action == data.allowed_actions[input.resource][_]
}