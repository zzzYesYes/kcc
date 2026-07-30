# app/storage.py
import os
import json
import aiofiles
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PolicyStorage:
    def __init__(self, base_path: str = "/app/data"):
        self.base_path = base_path
        self.templates_path = os.path.join(base_path, "templates")
        self.tenants_path = os.path.join(base_path, "tenants")

        # 确保目录存在
        os.makedirs(self.templates_path, exist_ok=True)
        os.makedirs(self.tenants_path, exist_ok=True)

    async def save_template(self, name: str, content: str) -> str:
        """保存Rego模板"""
        file_path = os.path.join(self.templates_path, f"{name}.rego")
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(content)
        return file_path

    async def get_template(self, name: str) -> Optional[str]:
        """获取Rego模板"""
        file_path = os.path.join(self.templates_path, f"{name}.rego")
        if not os.path.exists(file_path):
            return None
        async with aiofiles.open(file_path, 'r') as f:
            return await f.read()

    async def list_templates(self) -> List[str]:
        """列出所有模板"""
        files = os.listdir(self.templates_path)
        return [f.replace('.rego', '') for f in files if f.endswith('.rego')]

    async def save_tenant_policy(self, tenant_id: str, policy: Dict[str, Any]) -> str:
        """保存租户策略"""
        tenant_dir = os.path.join(self.tenants_path, tenant_id)
        os.makedirs(tenant_dir, exist_ok=True)

        policy_id = f"{policy['role']}_{policy['resource']}_{policy['action']}"
        file_path = os.path.join(tenant_dir, f"{policy_id}.json")

        policy_data = {
            **policy,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }

        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(policy_data, indent=2))

        return policy_id

    async def get_tenant_policies(self, tenant_id: str) -> List[Dict[str, Any]]:
        """获取租户所有策略"""
        tenant_dir = os.path.join(self.tenants_path, tenant_id)
        if not os.path.exists(tenant_dir):
            return []

        policies = []
        for filename in os.listdir(tenant_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(tenant_dir, filename)
                async with aiofiles.open(file_path, 'r') as f:
                    content = await f.read()
                    policies.append(json.loads(content))

        return policies

    async def update_tenant_policy(
        self, tenant_id: str, policy_id: str, policy: Dict[str, Any]
    ) -> bool:
        """Update an existing tenant policy. Returns False if not found."""
        file_path = os.path.join(self.tenants_path, tenant_id, f"{policy_id}.json")
        if not os.path.exists(file_path):
            return False

        async with aiofiles.open(file_path, 'r') as f:
            existing = json.loads(await f.read())

        updated = {
            **existing,
            **policy,
            "updated_at": datetime.utcnow().isoformat(),
        }
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(updated, indent=2))
        return True

    async def delete_tenant_policy(self, tenant_id: str, policy_id: str) -> bool:
        """Delete a tenant policy. Returns False if not found."""
        file_path = os.path.join(self.tenants_path, tenant_id, f"{policy_id}.json")
        if not os.path.exists(file_path):
            return False
        os.remove(file_path)
        return True

    async def generate_bundle(self, tenant_id: str) -> Dict[str, Any]:
        """生成OPA bundle数据"""
        policies = await self.get_tenant_policies(tenant_id)

        data = {
            "policies": {},
            "roles": {},
            "resources": {}
        }

        for policy in policies:
            role = policy["role"]
            resource = policy["resource"]
            action = policy["action"]

            if role not in data["roles"]:
                data["roles"][role] = {}
            if resource not in data["roles"][role]:
                data["roles"][role][resource] = []

            data["roles"][role][resource].append(action)

            policy_key = f"{role}_{resource}_{action}"
            data["policies"][policy_key] = policy

        return data
