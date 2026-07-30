#!/usr/bin/env python3
import os
import time
import json
import requests
import secrets
import base64
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# ===================== 配置项（从环境变量读取，无需硬编码） =====================
# Keycloak连接配置
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_HEALTH_URL = os.getenv("KEYCLOAK_HEALTH_URL", "http://keycloak:9000")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

# 超级管理员配置
SUPER_ADMIN_USER = os.getenv("SUPER_ADMIN_USER", "super-admin")
SUPER_ADMIN_INIT_PASSWORD = os.getenv("SUPER_ADMIN_INIT_PASSWORD", "SuperInit@123")

# 高权限Client配置
IDB_PROXY_CLIENT_ID = os.getenv("IDB_PROXY_CLIENT_ID", "idb-proxy-client")
# K8s Secret名称（用于存储Client Secret）
K8S_SECRET_NAME = os.getenv("K8S_SECRET_NAME", "keycloak-idb-proxy-client")
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "keycloak")

# ===================== 工具函数：等待Keycloak就绪 =====================
def wait_for_keycloak():
    """等待Keycloak健康检查接口返回成功（9000端口）"""
    health_url = f"{KEYCLOAK_HEALTH_URL}/health/ready"
    print(f"[Step 1/5] 等待Keycloak服务就绪：{health_url}", flush=True)
    
    max_retries = 50  # 最多等待250秒（50*5）
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            resp = requests.get(health_url, timeout=5)
            if resp.status_code == 200:
                print(f"[Step 1/5] Keycloak服务已就绪！", flush=True)
                return True
            else:
                print(f"[Step 1/5] Keycloak健康检查返回状态码：{resp.status_code}，等待中...（{retry_count+1}/{max_retries}）", flush=True)
        except requests.exceptions.ConnectionError:
            print(f"[Step 1/5] Keycloak未启动（连接拒绝），等待中...（{retry_count+1}/{max_retries}）", flush=True)
        except Exception as e:
            print(f"[Step 1/5] 检查Keycloak状态失败：{str(e)}，等待中...（{retry_count+1}/{max_retries}）", flush=True)
        
        retry_count += 1
        time.sleep(5)
    
    raise Exception(f"[Step 1/5] Keycloak服务超时未就绪（{max_retries*5}秒）")

# ===================== 工具函数：获取Keycloak Admin Token =====================
def get_keycloak_token():
    """调用Keycloak OpenID接口获取管理员Token"""
    print(f"[Step 2/5] 获取Keycloak Admin Token...", flush=True)
    
    url = f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token"
    data = {
        "username": ADMIN_USER,
        "password": ADMIN_PASSWORD,
        "grant_type": "password",
        "client_id": "admin-cli"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    
    try:
        resp = requests.post(url, data=data, headers=headers, timeout=10)
        resp.raise_for_status()
        token_data = resp.json()
        token = token_data["access_token"]
        print(f"[Step 2/5] 成功获取Keycloak Admin Token（有效期：{token_data['expires_in']}秒）", flush=True)
        return token
    except Exception as e:
        error_detail = resp.text if 'resp' in locals() else '无响应内容'
        raise Exception(f"[Step 2/5] 获取Token失败：{str(e)}，响应内容：{error_detail}")

# ===================== 工具函数：初始化K8s客户端 =====================
def init_k8s_client():
    """初始化K8s客户端（Pod内使用ServiceAccount自动配置）"""
    print(f"[Tool] 初始化K8s客户端...", flush=True)
    try:
        # Pod内自动加载ServiceAccount配置（/var/run/secrets/kubernetes.io/serviceaccount/）
        config.load_incluster_config()
        v1_api = client.CoreV1Api()
        print(f"[Tool] K8s客户端初始化成功", flush=True)
        return v1_api
    except Exception as e:
        raise Exception(f"[Tool] 初始化K8s客户端失败：{str(e)}")

# ===================== 工具函数：创建/更新K8s Secret =====================
def create_or_update_k8s_secret(secret_data):
    """
    创建/更新K8s Secret
    :param secret_data: dict，如 {"client-id": "xxx", "client-secret": "xxx"}
    """
    print(f"[Tool] 准备将数据存入K8s Secret：{K8S_SECRET_NAME}（命名空间：{K8S_NAMESPACE}）", flush=True)
    
    # 初始化K8s客户端
    v1_api = init_k8s_client()
    
    # K8s Secret要求数据为base64编码
    encoded_data = {}
    for key, value in secret_data.items():
        b64_value = base64.b64encode(value.encode("utf-8")).decode("utf-8")
        encoded_data[key] = b64_value
    
    try:
        # 先尝试读取现有Secret
        existing_secret = v1_api.read_namespaced_secret(K8S_SECRET_NAME, K8S_NAMESPACE)
        # 更新现有Secret
        existing_secret.data = encoded_data
        v1_api.patch_namespaced_secret(
            name=K8S_SECRET_NAME,
            namespace=K8S_NAMESPACE,
            body=existing_secret
        )
        print(f"[Tool] 成功更新K8s Secret：{K8S_SECRET_NAME}", flush=True)
        return True
    except ApiException as e:
        if e.status == 404:
            # 不存在则创建新Secret
            secret = client.V1Secret(
                api_version="v1",
                kind="Secret",
                metadata=client.V1ObjectMeta(
                    name=K8S_SECRET_NAME,
                    namespace=K8S_NAMESPACE,
                    labels={"app": "keycloak", "component": "idb-proxy-client"}
                ),
                type="Opaque",
                data=encoded_data
            )
            v1_api.create_namespaced_secret(
                namespace=K8S_NAMESPACE,
                body=secret
            )
            print(f"[Tool] 成功创建K8s Secret：{K8S_SECRET_NAME}", flush=True)
            return True
        else:
            raise Exception(f"[Tool] 操作K8s Secret失败：{str(e)}（状态码：{e.status}）")
    except Exception as e:
        raise Exception(f"[Tool] 操作K8s Secret失败：{str(e)}")

# ===================== 核心功能1：创建超级管理员（首次登录强制改密码） =====================
def create_super_admin(token):
    """创建超级管理员用户，并分配权限"""
    print(f"[Step 3/5] 创建超级管理员用户：{SUPER_ADMIN_USER}...", flush=True)
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # 1. 检查用户是否已存在
    user_search_url = f"{KEYCLOAK_URL}/admin/realms/master/users?username={SUPER_ADMIN_USER}"
    resp = requests.get(user_search_url, headers=headers, timeout=10)
    resp.raise_for_status()
    
    if len(resp.json()) > 0:
        user_id = resp.json()[0]["id"]
        print(f"[Step 3/5] 超级管理员用户已存在（ID：{user_id}），跳过创建", flush=True)
    else:
        # 2. 创建用户
        user_url = f"{KEYCLOAK_URL}/admin/realms/master/users"
        user_data = {
            "username": SUPER_ADMIN_USER,
            "enabled": True,
            "emailVerified": True,
            "firstName": "Super",
            "lastName": "Admin"
        }
        user_resp = requests.post(user_url, json=user_data, headers=headers, timeout=10)
        if user_resp.status_code not in [201, 200]:
            raise Exception(f"[Step 3/5] 创建超级管理员用户失败：{user_resp.text}")
        
        # 从Location Header获取用户ID
        user_id = user_resp.headers["Location"].split("/")[-1]
        print(f"[Step 3/5] 成功创建超级管理员用户（ID：{user_id}）", flush=True)
    
    # 3. 设置初始密码（temporary=True：首次登录强制改密码）
    pwd_url = f"{KEYCLOAK_URL}/admin/realms/master/users/{user_id}/reset-password"
    pwd_data = {
        "type": "password",
        "value": SUPER_ADMIN_INIT_PASSWORD,
        "temporary": True  # 关键：首次登录强制修改密码
    }
    pwd_resp = requests.put(pwd_url, json=pwd_data, headers=headers, timeout=10)
    if pwd_resp.status_code not in [204, 200]:
        raise Exception(f"[Step 3/5] 设置超级管理员密码失败：{pwd_resp.text}")
    
    # 4. 获取所有核心管理员角色ID
    def get_role_id(role_name):
        role_list_url = f"{KEYCLOAK_URL}/admin/realms/master/roles"
        role_resp = requests.get(role_list_url, headers=headers, timeout=10)
        role_resp.raise_for_status()
        for role in role_resp.json():
            if role["name"] == role_name:
                return role["id"]
        return None
    
    admin_role_id = get_role_id("admin")
    create_realm_role_id = get_role_id("create-realm")
    
    # 5. 给用户分配管理员角色
    if admin_role_id or create_realm_role_id:
        role_mapping_url = f"{KEYCLOAK_URL}/admin/realms/master/users/{user_id}/role-mappings/realm"
        roles = []
        # if admin_role_id:
        #     roles.append({"id": admin_role_id, "name": "admin"})
        if create_realm_role_id:
            roles.append({"id": create_realm_role_id, "name": "create-realm"})
        
        role_resp = requests.post(role_mapping_url, json=roles, headers=headers, timeout=10)
        if role_resp.status_code in [204, 200]:
            print(f"[Step 3/5] 成功给超级管理员分配角色：{[r['name'] for r in roles]}", flush=True)
    
    print(f"[Step 3/5] 超级管理员配置完成！")
    print(f"[Step 3/5] 用户名：{SUPER_ADMIN_USER}")
    print(f"[Step 3/5] 初始密码：{SUPER_ADMIN_INIT_PASSWORD}（首次登录需强制修改）", flush=True)
    return user_id

# ===================== 核心功能2：创建IDB Proxy Client并存储Secret到K8s =====================
def create_idb_proxy_client(token):
    """创建Client，生成Secret，并将Secret存入K8s Secret"""
    print(f"[Step 4/5] 创建高权限Client：{IDB_PROXY_CLIENT_ID}...", flush=True)
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # 1. 检查Client是否已存在
    client_search_url = f"{KEYCLOAK_URL}/admin/realms/master/clients?clientId={IDB_PROXY_CLIENT_ID}"
    resp = requests.get(client_search_url, headers=headers, timeout=10)
    resp.raise_for_status()
    
    if len(resp.json()) > 0:
        # Client已存在，获取现有Secret
        client_info = resp.json()[0]
        client_id = client_info["id"]
        print(f"[Step 4/5] Client已存在（ID：{client_id}），获取现有Secret...", flush=True)
        
        # 获取Client Secret
        secret_url = f"{KEYCLOAK_URL}/admin/realms/master/clients/{client_id}/client-secret"
        secret_resp = requests.get(secret_url, headers=headers, timeout=10)
        secret_resp.raise_for_status()
        client_secret = secret_resp.json()["value"]
    else:
        # 2. 创建新的高权限Client
        client_url = f"{KEYCLOAK_URL}/admin/realms/master/clients"
        client_data = {
            "clientId": IDB_PROXY_CLIENT_ID,
            "name": "IDB Proxy Client (Auto-created)",
            "enabled": True,
            "clientAuthenticatorType": "client-secret",
            "redirectUris": ["*"],  # 生产环境请替换为具体URL
            "webOrigins": ["*"],     # 生产环境请替换为具体URL
            "serviceAccountsEnabled": True,
            "directAccessGrantsEnabled": True,
            "standardFlowEnabled": True,
            "implicitFlowEnabled": False,
            "publicClient": False,
            "bearerOnly": False
        }
        client_resp = requests.post(client_url, json=client_data, headers=headers, timeout=10)
        if client_resp.status_code not in [201, 200]:
            raise Exception(f"[Step 4/5] 创建Client失败：{client_resp.text}")
        
        # 获取新创建的Client ID
        client_id = client_resp.headers["Location"].split("/")[-1]
        print(f"[Step 4/5] 成功创建Client（ID：{client_id}）", flush=True)
        
        # 3. 生成Client Secret
        secret_url = f"{KEYCLOAK_URL}/admin/realms/master/clients/{client_id}/client-secret"
        secret_resp = requests.post(secret_url, headers=headers, timeout=10)
        secret_resp.raise_for_status()
        client_secret = secret_resp.json()["value"]
        print(f"[Step 4/5] 成功生成Client Secret", flush=True)
        
        # 4. 给Client的服务账号分配管理员角色
        service_account_url = f"{KEYCLOAK_URL}/admin/realms/master/clients/{client_id}/service-account-user"
        sa_resp = requests.get(service_account_url, headers=headers, timeout=10)
        sa_resp.raise_for_status()
        sa_user_id = sa_resp.json()["id"]
        
        # 获取管理员角色ID
        def get_role_id(role_name):
            role_list_url = f"{KEYCLOAK_URL}/admin/realms/master/roles"
            role_resp = requests.get(role_list_url, headers=headers, timeout=10)
            role_resp.raise_for_status()
            for role in role_resp.json():
                if role["name"] == role_name:
                    return role["id"]
            return None
        
        admin_role_id = get_role_id("admin")
        if admin_role_id:
            role_mapping_url = f"{KEYCLOAK_URL}/admin/realms/master/users/{sa_user_id}/role-mappings/realm"
            role_resp = requests.post(
                role_mapping_url,
                json=[{"id": admin_role_id, "name": "admin"}],
                headers=headers,
                timeout=10
            )
            if role_resp.status_code in [204, 200]:
                print(f"[Step 4/5] 成功给Client服务账号分配admin角色", flush=True)
    
    # 5. 将Client ID和Secret存入K8s Secret
    print(f"[Step 4/5] 准备将Client Secret存入K8s Secret...", flush=True)
    create_or_update_k8s_secret({
        "client-id": IDB_PROXY_CLIENT_ID,
        "client-secret": client_secret,
        "keycloak-url": KEYCLOAK_URL,
        "created-at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    })
    
    print(f"[Step 4/5] Client配置完成！")
    print(f"[Step 4/5] Client ID：{IDB_PROXY_CLIENT_ID}")
    print(f"[Step 4/5] Client Secret：{client_secret}")
    print(f"[Step 4/5] 已存入K8s Secret：{K8S_NAMESPACE}/{K8S_SECRET_NAME}", flush=True)
    return client_id, client_secret

# ===================== 主函数 =====================
def main():
    """主执行流程"""
    try:
        # Step 1：等待Keycloak就绪
        wait_for_keycloak()
        
        # Step 2：获取Admin Token
        token = get_keycloak_token()
        
        # Step 3：创建超级管理员
        create_super_admin(token)
        
        # Step 4：创建Client并存储Secret
        create_idb_proxy_client(token)
        
        # Step 5：完成
        print("\n" + "="*80, flush=True)
        print(f"🎉 所有初始化操作完成！", flush=True)
        print(f"📌 超级管理员：{SUPER_ADMIN_USER}（初始密码：{SUPER_ADMIN_INIT_PASSWORD}，首次登录需修改）", flush=True)
        print(f"📌 Client：{IDB_PROXY_CLIENT_ID}", flush=True)
        print(f"📌 K8s Secret：{K8S_NAMESPACE}/{K8S_SECRET_NAME}", flush=True)
        print("="*80 + "\n", flush=True)
        
        return 0
    
    except Exception as e:
        print(f"\n❌ 初始化失败：{str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
