package authz.templates.role_based

default allow = false

allow {
    input.tenant_id == input.tenant_id
    contains(input.roles[_], input.role)
    input.resource == input.resource
    input.action == input.action
}