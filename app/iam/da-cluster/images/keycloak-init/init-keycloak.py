#!/usr/bin/env python3
import os
import time
import json
import requests
import secrets
import base64
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# ===================== Configuration =====================
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_HEALTH_URL = os.getenv("KEYCLOAK_HEALTH_URL", "http://keycloak:9000")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

SUPER_ADMIN_USER = os.getenv("SUPER_ADMIN_USER", "super-admin")
SUPER_ADMIN_INIT_PASSWORD = os.getenv("SUPER_ADMIN_INIT_PASSWORD", "SuperInit@123")

IDB_PROXY_CLIENT_ID = os.getenv("IDB_PROXY_CLIENT_ID", "idb-proxy-client")
K8S_SECRET_NAME = os.getenv("K8S_SECRET_NAME", "keycloak-idb-proxy-client")
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "keycloak")

# Default tenant configuration
DEFAULT_TENANT_REALM = os.getenv("DEFAULT_TENANT_REALM", "data-agent")
DEFAULT_TENANT_ADMIN_USER = os.getenv("DEFAULT_TENANT_ADMIN_USER", "tenant-admin")
DEFAULT_TENANT_ADMIN_PASSWORD = os.getenv("DEFAULT_TENANT_ADMIN_PASSWORD", "TenantAdmin@123")
DEFAULT_TENANT_NORMAL_USER = os.getenv("DEFAULT_TENANT_NORMAL_USER", "normal-user")
DEFAULT_TENANT_NORMAL_PASSWORD = os.getenv("DEFAULT_TENANT_NORMAL_PASSWORD", "NormalUser@123")

# ===================== Utility: Wait for Keycloak =====================
def wait_for_keycloak():
    health_url = f"{KEYCLOAK_HEALTH_URL}/health/ready"
    print(f"[Step 1/8] Waiting for Keycloak: {health_url}", flush=True)
    max_retries = 50
    for i in range(max_retries):
        try:
            resp = requests.get(health_url, timeout=5)
            if resp.status_code == 200:
                print(f"[Step 1/8] Keycloak is ready!", flush=True)
                return True
            print(f"[Step 1/8] Health check returned {resp.status_code}, waiting... ({i+1}/{max_retries})", flush=True)
        except requests.exceptions.ConnectionError:
            print(f"[Step 1/8] Keycloak not up yet, waiting... ({i+1}/{max_retries})", flush=True)
        except Exception as e:
            print(f"[Step 1/8] Health check error: {e}, waiting... ({i+1}/{max_retries})", flush=True)
        time.sleep(5)
    raise Exception(f"Keycloak not ready after {max_retries*5}s")

# ===================== Utility: Get Admin Token =====================
def get_keycloak_token():
    print(f"[Step 2/8] Getting Keycloak Admin Token...", flush=True)
    url = f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token"
    data = {
        "username": ADMIN_USER,
        "password": ADMIN_PASSWORD,
        "grant_type": "password",
        "client_id": "admin-cli"
    }
    resp = requests.post(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=10)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print(f"[Step 2/8] Got admin token (expires in {resp.json()['expires_in']}s)", flush=True)
    return token

# ===================== Utility: K8s Client =====================
def init_k8s_client():
    config.load_incluster_config()
    return client.CoreV1Api()

def create_or_update_k8s_secret(secret_data):
    print(f"[Tool] Saving K8s Secret: {K8S_SECRET_NAME} (ns: {K8S_NAMESPACE})", flush=True)
    v1_api = init_k8s_client()
    encoded_data = {k: base64.b64encode(v.encode("utf-8")).decode("utf-8") for k, v in secret_data.items()}
    try:
        existing = v1_api.read_namespaced_secret(K8S_SECRET_NAME, K8S_NAMESPACE)
        existing.data = encoded_data
        v1_api.patch_namespaced_secret(K8S_SECRET_NAME, K8S_NAMESPACE, existing)
        print(f"[Tool] Updated K8s Secret: {K8S_SECRET_NAME}", flush=True)
    except ApiException as e:
        if e.status == 404:
            secret = client.V1Secret(
                api_version="v1", kind="Secret",
                metadata=client.V1ObjectMeta(name=K8S_SECRET_NAME, namespace=K8S_NAMESPACE,
                    labels={"app": "keycloak", "component": "idb-proxy-client"}),
                type="Opaque", data=encoded_data
            )
            v1_api.create_namespaced_secret(K8S_NAMESPACE, secret)
            print(f"[Tool] Created K8s Secret: {K8S_SECRET_NAME}", flush=True)
        else:
            raise

# ===================== Keycloak API helpers =====================
def kc_headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def get_realm_role(token, realm, role_name):
    """Get a realm role by name, returns dict or None."""
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/roles/{role_name}"
    resp = requests.get(url, headers=kc_headers(token), timeout=10)
    if resp.status_code == 200:
        return resp.json()
    return None

def create_realm_role(token, realm, role_name, description=""):
    """Create a realm role if it doesn't exist. Returns role dict."""
    existing = get_realm_role(token, realm, role_name)
    if existing:
        print(f"  Role '{role_name}' already exists in realm '{realm}'", flush=True)
        return existing
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/roles"
    resp = requests.post(url, json={"name": role_name, "description": description},
                         headers=kc_headers(token), timeout=10)
    if resp.status_code not in [201, 200, 409]:
        raise Exception(f"Failed to create role '{role_name}': {resp.text}")
    role = get_realm_role(token, realm, role_name)
    print(f"  Created role '{role_name}' in realm '{realm}'", flush=True)
    return role

def find_user(token, realm, username):
    """Find a user by username. Returns user dict or None."""
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users?username={username}&exact=true"
    resp = requests.get(url, headers=kc_headers(token), timeout=10)
    resp.raise_for_status()
    users = resp.json()
    return users[0] if users else None

def create_user(token, realm, username, password, first_name="", last_name="",
                email="", temporary_password=False):
    """Create a user if not exists. Returns user_id."""
    if not email:
        email = f"{username}@{realm}.local"
    existing = find_user(token, realm, username)
    if existing:
        user_id = existing["id"]
        print(f"  User '{username}' already exists (id: {user_id})", flush=True)
        # Ensure email is set (Keycloak 26 requires it for user profile validation)
        if not existing.get("email"):
            update_url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users/{user_id}"
            requests.put(update_url, json={"email": email, "emailVerified": True},
                         headers=kc_headers(token), timeout=10)
    else:
        url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users"
        user_data = {
            "username": username, "enabled": True, "emailVerified": True,
            "firstName": first_name, "lastName": last_name,
            "email": email
        }
        resp = requests.post(url, json=user_data, headers=kc_headers(token), timeout=10)
        if resp.status_code not in [201, 200]:
            raise Exception(f"Failed to create user '{username}': {resp.text}")
        user_id = resp.headers["Location"].split("/")[-1]
        print(f"  Created user '{username}' (id: {user_id})", flush=True)

    # Set password
    pwd_url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users/{user_id}/reset-password"
    pwd_data = {"type": "password", "value": password, "temporary": temporary_password}
    resp = requests.put(pwd_url, json=pwd_data, headers=kc_headers(token), timeout=10)
    if resp.status_code not in [204, 200]:
        raise Exception(f"Failed to set password for '{username}': {resp.text}")
    return user_id

def assign_realm_roles(token, realm, user_id, roles):
    """Assign realm roles to a user. roles = list of {"id": ..., "name": ...}"""
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users/{user_id}/role-mappings/realm"
    resp = requests.post(url, json=roles, headers=kc_headers(token), timeout=10)
    if resp.status_code in [204, 200]:
        print(f"  Assigned roles {[r['name'] for r in roles]} to user", flush=True)
    else:
        print(f"  Warning: role assignment returned {resp.status_code}: {resp.text}", flush=True)

# ===================== Utility: Create Data Agent Script Mapper (320 kc-spi) =====================
def create_roles_mapper(token, realm, client_internal_id):
    """
    Create Data Agent Script Mapper that outputs realm_id + structured roles
    [{id, name}] in JWT tokens. Requires keycloak-custom image with
    data-agent-mapper.jar in /opt/keycloak/providers/ and --features=scripts.
    """
    headers = kc_headers(token)
    mapper_name = "data-agent-mapper"

    # Check if mapper already exists
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/clients/{client_internal_id}/protocol-mappers/models"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    for m in resp.json():
        if m.get("name") == mapper_name:
            print(f"  Mapper '{mapper_name}' already exists in realm '{realm}'", flush=True)
            return

    mapper = {
        "name": mapper_name,
        "protocol": "openid-connect",
        "protocolMapper": "script-data-agent-mapper.js",
        "config": {
            "access.token.claim": "true",
            "id.token.claim": "true",
            "userinfo.token.claim": "true",
        }
    }
    resp = requests.post(url, json=mapper, headers=headers, timeout=10)
    if resp.status_code in [201, 200]:
        print(f"  Created Script Mapper '{mapper_name}' in realm '{realm}'", flush=True)
    else:
        print(f"  Warning: mapper creation returned {resp.status_code}: {resp.text}", flush=True)

def ensure_role_hyphen(token, realm, correct_name):
    """Ensure role uses hyphen naming. Delete old underscore variant if it exists."""
    underscore_name = correct_name.replace("-", "_")
    if underscore_name != correct_name:
        old_role = get_realm_role(token, realm, underscore_name)
        if old_role:
            url = f"{KEYCLOAK_URL}/admin/realms/{realm}/roles/{underscore_name}"
            resp = requests.delete(url, headers=kc_headers(token), timeout=10)
            if resp.status_code in [204, 200]:
                print(f"  Removed old underscore role '{underscore_name}' in realm '{realm}'", flush=True)
    return create_realm_role(token, realm, correct_name)

# ===================== Step 3: Create super_admin role + super-admin user =====================
def setup_super_admin(token):
    print(f"[Step 3/8] Setting up super_admin role and user in master realm...", flush=True)

    # Create roles with hyphen naming (remove underscore variants if they exist)
    super_admin_role = ensure_role_hyphen(token, "master", "super-admin")
    ensure_role_hyphen(token, "master", "tenant-admin")

    # Create super-admin user (temporary_password=False for automated testing;
    # in production, set to True so user must change on first login)
    user_id = create_user(token, "master", SUPER_ADMIN_USER, SUPER_ADMIN_INIT_PASSWORD,
                          first_name="Super", last_name="Admin",
                          email=f"{SUPER_ADMIN_USER}@master.local",
                          temporary_password=False)

    # Assign super_admin + create-realm roles
    create_realm_role_obj = get_realm_role(token, "master", "create-realm")
    roles_to_assign = []
    if super_admin_role:
        roles_to_assign.append({"id": super_admin_role["id"], "name": "super-admin"})
    if create_realm_role_obj:
        roles_to_assign.append({"id": create_realm_role_obj["id"], "name": "create-realm"})
    if roles_to_assign:
        assign_realm_roles(token, "master", user_id, roles_to_assign)

    print(f"[Step 3/8] Super admin setup complete: {SUPER_ADMIN_USER}", flush=True)
    return user_id

# ===================== Step 4: Create IDB Proxy Client =====================
def create_idb_proxy_client(token):
    print(f"[Step 4/8] Creating service account client: {IDB_PROXY_CLIENT_ID}...", flush=True)
    headers = kc_headers(token)

    # Check if client exists
    search_url = f"{KEYCLOAK_URL}/admin/realms/master/clients?clientId={IDB_PROXY_CLIENT_ID}"
    resp = requests.get(search_url, headers=headers, timeout=10)
    resp.raise_for_status()

    existing_client = False
    if resp.json():
        existing_client = True
        client_info = resp.json()[0]
        client_id = client_info["id"]
        print(f"  Client already exists (id: {client_id})", flush=True)
        secret_url = f"{KEYCLOAK_URL}/admin/realms/master/clients/{client_id}/client-secret"
        secret_resp = requests.get(secret_url, headers=headers, timeout=10)
        secret_resp.raise_for_status()
        client_secret = secret_resp.json()["value"]
    else:
        client_url = f"{KEYCLOAK_URL}/admin/realms/master/clients"
        client_data = {
            "clientId": IDB_PROXY_CLIENT_ID,
            "name": "IDB Proxy Client (Auto-created)",
            "enabled": True,
            "clientAuthenticatorType": "client-secret",
            "redirectUris": ["*"], "webOrigins": ["*"],
            "serviceAccountsEnabled": True,
            "directAccessGrantsEnabled": True,
            "standardFlowEnabled": True,
            "implicitFlowEnabled": False,
            "publicClient": False,
            "bearerOnly": False
        }
        resp = requests.post(client_url, json=client_data, headers=headers, timeout=10)
        if resp.status_code not in [201, 200]:
            raise Exception(f"Failed to create client: {resp.text}")
        client_id = resp.headers["Location"].split("/")[-1]
        print(f"  Created client (id: {client_id})", flush=True)

        # Generate secret
        secret_url = f"{KEYCLOAK_URL}/admin/realms/master/clients/{client_id}/client-secret"
        secret_resp = requests.post(secret_url, headers=headers, timeout=10)
        secret_resp.raise_for_status()
        client_secret = secret_resp.json()["value"]

        # Assign admin role to service account (needed for Keycloak Admin API calls)
        sa_url = f"{KEYCLOAK_URL}/admin/realms/master/clients/{client_id}/service-account-user"
        sa_resp = requests.get(sa_url, headers=headers, timeout=10)
        sa_resp.raise_for_status()
        sa_user_id = sa_resp.json()["id"]

        admin_role = get_realm_role(token, "master", "admin")
        super_admin_role = get_realm_role(token, "master", "super-admin")
        sa_roles = []
        if admin_role:
            sa_roles.append({"id": admin_role["id"], "name": "admin"})
        if super_admin_role:
            sa_roles.append({"id": super_admin_role["id"], "name": "super-admin"})
        if sa_roles:
            assign_realm_roles(token, "master", sa_user_id, sa_roles)

    # For existing clients, ensure service account has super_admin role
    if existing_client:
        sa_url = f"{KEYCLOAK_URL}/admin/realms/master/clients/{client_id}/service-account-user"
        sa_resp = requests.get(sa_url, headers=headers, timeout=10)
        sa_resp.raise_for_status()
        sa_user_id = sa_resp.json()["id"]
        super_admin_role = get_realm_role(token, "master", "super-admin")
        if super_admin_role:
            assign_realm_roles(token, "master", sa_user_id,
                               [{"id": super_admin_role["id"], "name": "super-admin"}])

    # Store in K8s Secret
    create_or_update_k8s_secret({
        "client-id": IDB_PROXY_CLIENT_ID,
        "client-secret": client_secret,
        "keycloak-url": KEYCLOAK_URL,
        "created-at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    })
    print(f"[Step 4/8] Client setup complete: {IDB_PROXY_CLIENT_ID}", flush=True)
    return client_id, client_secret

# ===================== Step 5: Create default tenant realm =====================
def create_default_tenant(token):
    realm = DEFAULT_TENANT_REALM
    print(f"[Step 5/8] Creating default tenant realm: {realm}...", flush=True)
    headers = kc_headers(token)

    # Check if realm exists
    resp = requests.get(f"{KEYCLOAK_URL}/admin/realms/{realm}", headers=headers, timeout=10)
    if resp.status_code == 200:
        print(f"  Realm '{realm}' already exists, skipping creation", flush=True)
    else:
        # Create realm
        realm_data = {
            "realm": realm,
            "displayName": "Data Agent (Default Tenant)",
            "enabled": True,
            "registrationAllowed": False,
            "loginWithEmailAllowed": True,
            "duplicateEmailsAllowed": False,
            "resetPasswordAllowed": True,
            "editUsernameAllowed": False,
            "bruteForceProtected": True
        }
        resp = requests.post(f"{KEYCLOAK_URL}/admin/realms", json=realm_data,
                             headers=headers, timeout=10)
        if resp.status_code not in [201, 200]:
            raise Exception(f"Failed to create realm '{realm}': {resp.text}")
        print(f"  Created realm '{realm}'", flush=True)

    # Create roles with hyphen naming (remove underscore variants if they exist)
    ensure_role_hyphen(token, realm, "super-admin")
    ensure_role_hyphen(token, realm, "tenant-admin")
    ensure_role_hyphen(token, realm, "normal-user")

    # Create tenant_admin user
    ta_user_id = create_user(token, realm, DEFAULT_TENANT_ADMIN_USER,
                             DEFAULT_TENANT_ADMIN_PASSWORD,
                             first_name="Tenant", last_name="Admin",
                             temporary_password=False)
    ta_role = get_realm_role(token, realm, "tenant-admin")
    if ta_role:
        assign_realm_roles(token, realm, ta_user_id, [{"id": ta_role["id"], "name": "tenant-admin"}])

    # Create normal user
    nu_user_id = create_user(token, realm, DEFAULT_TENANT_NORMAL_USER,
                             DEFAULT_TENANT_NORMAL_PASSWORD,
                             first_name="Normal", last_name="User",
                             temporary_password=False)
    nu_role = get_realm_role(token, realm, "normal-user")
    if nu_role:
        assign_realm_roles(token, realm, nu_user_id, [{"id": nu_role["id"], "name": "normal-user"}])

    print(f"[Step 5/8] Default tenant '{realm}' setup complete", flush=True)

# ===================== Step 6: Create client in tenant realm =====================
def create_tenant_client(token):
    realm = DEFAULT_TENANT_REALM
    client_id_name = realm
    print(f"[Step 6/8] Creating client '{client_id_name}' in realm '{realm}'...", flush=True)
    headers = kc_headers(token)

    search_url = f"{KEYCLOAK_URL}/admin/realms/{realm}/clients?clientId={client_id_name}"
    resp = requests.get(search_url, headers=headers, timeout=10)
    resp.raise_for_status()

    if resp.json():
        client_info = resp.json()[0]
        cid = client_info["id"]
        print(f"  Client '{client_id_name}' already exists (id: {cid})", flush=True)
        secret_url = f"{KEYCLOAK_URL}/admin/realms/{realm}/clients/{cid}/client-secret"
        secret_resp = requests.get(secret_url, headers=headers, timeout=10)
        secret_resp.raise_for_status()
        tenant_client_secret = secret_resp.json()["value"]
    else:
        client_data = {
            "clientId": client_id_name,
            "name": f"{realm} Tenant Client",
            "enabled": True,
            "clientAuthenticatorType": "client-secret",
            "redirectUris": ["*"], "webOrigins": ["*"],
            "serviceAccountsEnabled": False,
            "directAccessGrantsEnabled": True,
            "standardFlowEnabled": True,
            "publicClient": True,
            "bearerOnly": False
        }
        resp = requests.post(f"{KEYCLOAK_URL}/admin/realms/{realm}/clients",
                             json=client_data, headers=headers, timeout=10)
        if resp.status_code not in [201, 200]:
            raise Exception(f"Failed to create tenant client: {resp.text}")
        cid = resp.headers["Location"].split("/")[-1]
        print(f"  Created client '{client_id_name}' (id: {cid})", flush=True)

        secret_url = f"{KEYCLOAK_URL}/admin/realms/{realm}/clients/{cid}/client-secret"
        secret_resp = requests.post(secret_url, headers=headers, timeout=10)
        secret_resp.raise_for_status()
        tenant_client_secret = secret_resp.json()["value"]

    # Store tenant client secret in K8s
    tenant_secret_name = f"keycloak-{realm}-client"
    print(f"  Saving tenant client secret to K8s Secret: {tenant_secret_name}", flush=True)
    v1_api = init_k8s_client()
    encoded_data = {
        k: base64.b64encode(v.encode("utf-8")).decode("utf-8") for k, v in {
            "client-id": client_id_name,
            "client-secret": tenant_client_secret,
            "realm": realm,
        }.items()
    }
    try:
        existing = v1_api.read_namespaced_secret(tenant_secret_name, K8S_NAMESPACE)
        existing.data = encoded_data
        v1_api.patch_namespaced_secret(tenant_secret_name, K8S_NAMESPACE, existing)
    except ApiException as e:
        if e.status == 404:
            secret = client.V1Secret(
                api_version="v1", kind="Secret",
                metadata=client.V1ObjectMeta(name=tenant_secret_name, namespace=K8S_NAMESPACE,
                    labels={"app": "keycloak", "component": "tenant-client"}),
                type="Opaque", data=encoded_data
            )
            v1_api.create_namespaced_secret(K8S_NAMESPACE, secret)
        else:
            raise

    print(f"[Step 6/8] Tenant client setup complete", flush=True)
    return tenant_client_secret

# ===================== Step 7: Apply roles Script Mapper to clients =====================
def setup_roles_mapper(token):
    print(f"[Step 7/8] Setting up roles Script Protocol Mapper...", flush=True)
    headers = kc_headers(token)

    # Apply to idb-proxy-client in master realm
    search_url = f"{KEYCLOAK_URL}/admin/realms/master/clients?clientId={IDB_PROXY_CLIENT_ID}"
    resp = requests.get(search_url, headers=headers, timeout=10)
    resp.raise_for_status()
    if resp.json():
        master_client_id = resp.json()[0]["id"]
        create_roles_mapper(token, "master", master_client_id)

    # Apply to data-agent-client in tenant realm
    tenant_client_name = f"{DEFAULT_TENANT_REALM}-client"
    search_url = f"{KEYCLOAK_URL}/admin/realms/{DEFAULT_TENANT_REALM}/clients?clientId={tenant_client_name}"
    resp = requests.get(search_url, headers=headers, timeout=10)
    resp.raise_for_status()
    if resp.json():
        tenant_client_id = resp.json()[0]["id"]
        create_roles_mapper(token, DEFAULT_TENANT_REALM, tenant_client_id)

    print(f"[Step 7/8] Roles mapper setup complete", flush=True)

# ===================== Main =====================
def main():
    try:
        wait_for_keycloak()
        token = get_keycloak_token()
        setup_super_admin(token)
        create_idb_proxy_client(token)
        create_default_tenant(token)
        create_tenant_client(token)
        setup_roles_mapper(token)

        print("\n" + "="*80, flush=True)
        print(f"All initialization complete!", flush=True)
        print(f"  Super admin: {SUPER_ADMIN_USER} (password: {SUPER_ADMIN_INIT_PASSWORD}, must change on first login)", flush=True)
        print(f"  Service client: {IDB_PROXY_CLIENT_ID} (K8s Secret: {K8S_NAMESPACE}/{K8S_SECRET_NAME})", flush=True)
        print(f"  Default tenant: {DEFAULT_TENANT_REALM}", flush=True)
        print(f"    tenant-admin: {DEFAULT_TENANT_ADMIN_USER} / {DEFAULT_TENANT_ADMIN_PASSWORD}", flush=True)
        print(f"    normal-user: {DEFAULT_TENANT_NORMAL_USER} / {DEFAULT_TENANT_NORMAL_PASSWORD}", flush=True)
        print(f"    client: {DEFAULT_TENANT_REALM}-client (K8s Secret: {K8S_NAMESPACE}/keycloak-{DEFAULT_TENANT_REALM}-client)", flush=True)
        print("="*80 + "\n", flush=True)
        return 0
    except Exception as e:
        print(f"\nInitialization failed: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
