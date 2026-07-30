package authz.templates.resource_owner

default allow = false

allow {
    input.tenant_id == input.tenant_id
    input.resource == input.resource
    input.action == input.action
    input.user == data.resource_owners[input.resource]
}