import os
import requests
from typing import Optional


class OPAError(Exception):
    """OPA API 错误"""
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail


class OPAClient:
    """OPA (Open Policy Agent) 服务客户端"""

    def __init__(self):
        self.session = requests.Session()
        # 关键：不信任环境变量，避免被公司代理拦截
        self.session.trust_env = False

    @property
    def base_url(self):
        """延迟读取 OPA_BASE_URL，确保使用当前环境的配置"""
        return os.getenv("OPA_BASE_URL", "http://bundle-server.opa.svc.cluster.local:8001").rstrip('/')

    def request(self, method: str, path: str, **kwargs):
        """
        发送请求到 OPA 服务

        Args:
            method: HTTP 方法 (GET, POST, PUT, DELETE)
            path: API 路径，以 / 开头（例如 /api/v1/roles/{role_id}/policy）
            **kwargs: 传递给 requests.request 的其他参数 (params, json, data, headers 等)

        Returns:
            requests.Response 对象

        Raises:
            OPAError: 请求失败时抛出
        """
        clean_path = path.lstrip('/')
        url = f"{self.base_url}/{clean_path}"

        # DEBUG: 打印正在使用的 base_url 和完整 URL
        print(f"[OPA_DEBUG] OPAClient.base_url = {self.base_url}")
        print(f"[OPA_DEBUG] Request URL = {url}")
        print(f"[OPA_DEBUG] Environment OPA_BASE_URL = {os.getenv('OPA_BASE_URL')}")

        headers = kwargs.pop("headers", {})
        headers.update({
            "Content-Type": "application/json",
        })

        resp = self.session.request(method, url, headers=headers, **kwargs)

        if not resp.ok:
            print(f"OPA_ERROR: {method} {url} -> {resp.status_code}: {resp.text}")
            raise OPAError(resp.status_code, resp.text)

        return resp


opa = OPAClient()
