import os, time, requests, json
from fastapi import HTTPException


class KeycloakError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail


class KeycloakClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.trust_env = False
        self.base_url = os.getenv("KEYCLOAK_URL", "http://localhost:8080").rstrip('/')
        self.master_realm = os.getenv("KC_REALM", "master")
        self._token = None
        self._token_expires_at = 0

    def _get_token(self):
        '''
        # 先注释掉token缓存，避免创建realm后没有及时获取操作权限
        if self._token and time.time() < self._token_expires_at - 10:
            return self._token
        '''
        url = f"{self.base_url}/realms/{self.master_realm}/protocol/openid-connect/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": os.getenv("KC_CLIENT_ID"),
            "client_secret": os.getenv("KC_CLIENT_SECRET")
        }
        resp = self.session.post(url, data=data)
        if not resp.ok:
            print(f"TOKEN_ERROR: {resp.status_code} - {resp.text}")
            raise HTTPException(status_code=401, detail="Keycloak Admin Auth Failed")
        res = resp.json()
        self._token = res["access_token"]
        self._token_expires_at = time.time() + res.get("expires_in", 60)
        return self._token

    def request(self, method: str, path: str, **kwargs):
        """
        path 应以 / 开头，例如 /realms/my-realm/users
        内部自动补全 /admin
        """
        token = self._get_token()
        # 核心：确保 URL 只有一段 /admin
        clean_path = path.lstrip('/')
        if not clean_path.startswith("admin/"):
            url = f"{self.base_url}/admin/{clean_path}"
        else:
            url = f"{self.base_url}/{clean_path}"

        headers = kwargs.pop("headers", {})
        headers.update({"Authorization": f"Bearer {token}"})

        resp = self.session.request(method, url, headers=headers, **kwargs)

        if not resp.ok:
            # 记录日志方便排查
            print(f"KC_ERROR: {method} {url} -> {resp.status_code}: {resp.text}")
            raise KeycloakError(resp.status_code, resp.text)
        return resp


kc = KeycloakClient()
