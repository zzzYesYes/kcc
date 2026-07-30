import requests
import json
import uuid

# 配置信息
BASE_URL = "http://127.0.0.1:8090/api/v1"
TEST_REALM = f"realm-{uuid.uuid4().hex[:6]}"  # 生成随机名避免冲突
SAML_ALIAS = "saml-idp-test"


class KeycloakWrapperTester:
    def __init__(self):
        self.session = requests.Session()
        # 核心：禁用系统代理环境变量，解决公司内网拦截问题
        self.session.trust_env = False
        self.base_url = BASE_URL
        # --- 新增计数器 ---
        self.total_count = 0  # 已运行实例数
        self.passed_count = 0  # 通过实例数
        self.failed_cases = []  # 记录失败的案例名，方便回溯

    def log(self, step_name, res):
        self.total_count += 1
        status = res.status_code

        # 成功的定义：200, 201, 204 都算通
        if 200 <= status < 300:
            self.passed_count += 1
            print(f"✅ [PASS] {step_name} - Status: {status}")
        else:
            self.failed_cases.append(step_name)
            print(f"❌ [FAIL] {step_name} - Status: {status}")
            print(f"   Response: {res.text[:200]}")  # 打印前200个字符的错误信息

    def print_summary(self):
        """打印最终测试报告"""
        print("\n" + "=" * 40)
        print("         TEST SUMMARY REPORT")
        print("=" * 40)
        print(f"Total Instances Run: {self.total_count}")
        print(f"Passed Instances:    {self.passed_count}")
        print(f"Failed Instances:    {len(self.failed_cases)}")

        success_rate = (self.passed_count / self.total_count * 100) if self.total_count > 0 else 0
        print(f"Success Rate:        {success_rate:.2f}%")

        if self.failed_cases:
            print("-" * 20)
            print("Failed Steps List:")
            for case in self.failed_cases:
                print(f"  - {case}")
        print("=" * 40 + "\n")

    # --- 0. 健康检查 ---
    def test_health_check(self):
        print("\n=== [场景 1: 健康检查] ===")
        res = self.session.get(f"{self.base_url}/common/health")
        self.log("健康检查", res)
        if res.status_code == 200:
            data = res.json()
            print(f"   Status: {data.get('status')}, Timestamp: {data.get('timestamp')}")

    # --- 1. 租户管理场景 ---
    def test_tenant_management(self):
        print("\n=== [场景 1: 租户管理] ===")
        # 创建
        payload = {"realm": TEST_REALM, "displayName": "自动化测试租户"}
        res = self.session.post(f"{self.base_url}/tenants", json=payload)
        self.log("创建租户", res)

        # 列表
        res = self.session.get(f"{self.base_url}/tenants")
        self.log("查看租户列表", res)

    # --- 2. 角色管理场景 (带 Attributes) ---
    def test_role_management(self):
        print("\n=== [场景 2: 角色管理] ===")
        role_name = "test_business_role"

        # 创建角色
        payload = {
            "name": role_name,
            "description": "测试用角色说明",
            "attributes": {"level": ["gold"], "region": ["asia"]}
        }
        res = self.session.post(f"{self.base_url}/{TEST_REALM}/roles", json=payload)
        self.log("添加角色", res)

        # 获取角色详情
        res = self.session.get(f"{self.base_url}/{TEST_REALM}/roles/{role_name}")
        self.log("查看指定角色信息", res)

        # 更新角色属性
        update_payload = {
            "attributes": {"level": ["platinum"], "region": ["asia"], "tag": ["new"]}
        }
        res = self.session.put(f"{self.base_url}/{TEST_REALM}/roles/{role_name}", json=update_payload)
        self.log("更新角色属性", res)

        # 删除角色
        res = self.session.delete(f"{self.base_url}/{TEST_REALM}/roles/{role_name}")
        self.log("删除角色", res)

    # --- 3. IDP 管理场景 (适配单实例限制与 PUT 接口) ---
    def test_idp_management(self):
        print("\n=== [场景 3: IDP 管理 (单实例限制与更新)] ===")

        # 准备一个基础的 IDP 配置
        # 注意：不再在 URL 里传 SAML_ALIAS，由后端从环境变量取
        idp_payload = {
            "enabled": True,
            "config": {
                "singleSignOnServiceUrl": "https://mock-idp/sso",
                "entityId": "http://mock-idp"
            }
        }

        # 1. 第一次创建 (预期成功)
        res = self.session.post(f"{self.base_url}/{TEST_REALM}/idp/saml/instances", json=idp_payload)
        self.log("创建第一个 SAML 实例", res)

        # 记录实际使用的 Alias (后端返回的)
        actual_alias = res.json().get("alias") if res.status_code == 201 else "da-saml-idp"

        # 2. 第二次创建 (预期失败 - 400 Bad Request)
        res_fail = self.session.post(f"{self.base_url}/{TEST_REALM}/idp/saml/instances", json=idp_payload)
        if res_fail.status_code == 400:
            print(f"✅ [PASS] 验证单 Realm 唯一性拦截成功: {res_fail.json().get('detail')}")
            self.passed_count += 1
            self.total_count += 1
        else:
            self.log("验证单 Realm 唯一性拦截失败", res_fail)

        # 3. 测试 PUT 更新接口
        update_payload = {
            "enabled": False,  # 尝试禁用它
            "config": {
                "singleSignOnServiceUrl": "https://new-mock-idp/sso",
                "guiOrder": "1"
            }
        }
        # 修正后的测试调用
        res_put = self.session.put(f"{self.base_url}/{TEST_REALM}/idp/saml/instances", json=update_payload)
        self.log("更新已有 SAML 实例配置", res_put)

        # 4. 验证更新结果 (GET 检查)
        # 假设你的获取接口是 GET /saml/instances/{alias} 或类似
        res_get = self.session.get(f"{self.base_url}/{TEST_REALM}/idp/saml/instances")
        if res_get.status_code == 200:
            instances = res_get.json()
            # 找到我们那个 alias 的实例，检查 enabled 是否变为了 False
            target = next((i for i in instances if i['alias'] == actual_alias), None)
            if target and target.get('enabled') is False:
                print(f"✅ [PASS] 验证 PUT 更新内容生效")
                self.passed_count += 1
                self.total_count += 1
            else:
                print(f"❌ [FAIL] 验证 PUT 更新内容未生效")
                self.total_count += 1

        # 5. 清理 (删除这个 IDP 方便后续测试)
        # 注意：这里的 URL 路径需根据你实际的删除接口调整
        del_res = self.session.delete(f"{self.base_url}/{TEST_REALM}/idp/saml/instances/{actual_alias}")
        self.log("清理删除 SAML 实例", del_res)

    # --- 4. 群组与用户管理场景 (补全了增删改) ---
    def test_group_and_user_management(self):
        print("\n=== [场景 4: 群组与用户管理] ===")
        group_name = "test_engineering_group"

        # 1. 创建群组
        res = self.session.post(f"{self.base_url}/{TEST_REALM}/groups", json={"name": group_name})
        self.log("创建群组", res)

        # 2. 获取群组列表并提取 ID
        res = self.session.get(f"{self.base_url}/{TEST_REALM}/groups")
        self.log("查看群组列表", res)

        group_id = None
        if res.status_code == 200:
            groups = res.json()
            target = next((g for g in groups if g['name'] == group_name), None)
            if target:
                group_id = target['id']

        if group_id:
            # 3. 更新群组 (修改属性)
            update_payload = {"name": group_name, "attributes": {"dept_code": ["1024"]}}
            res = self.session.put(f"{self.base_url}/{TEST_REALM}/groups/{group_id}", json=update_payload)
            self.log("更新群组属性", res)

            # 4. 删除群组
            res = self.session.delete(f"{self.base_url}/{TEST_REALM}/groups/{group_id}")
            self.log("删除群组", res)

        # 5. 查看用户列表
        res = self.session.get(f"{self.base_url}/{TEST_REALM}/users")
        self.log("查看用户列表", res)

    # --- 5. 清理租户 ---
    def cleanup(self):
        print("\n=== [清理: 删除租户] ===")
        res = self.session.delete(f"{self.base_url}/tenants/{TEST_REALM}")
        self.log("删除测试租户", res)

    # --- 6. 导出 OpenAPI ---
    def test_export_spec(self):
        print("\n=== [场景 5: 导出定义文件] ===")
        res = self.session.get(f"{self.base_url}/export-spec")
        self.log("获取OpenAPI定义", res)
        if res.status_code == 200:
            with open("keycloak_api_spec.json", "w", encoding="utf-8") as f:
                json.dump(res.json(), f, indent=2, ensure_ascii=False)
            print("OpenAPI JSON 已导出至当前目录")


def run_all():
    tester = KeycloakWrapperTester()
    try:
        tester.test_health_check()
        tester.test_tenant_management()
        tester.test_role_management()
        tester.test_idp_management()
        tester.test_group_and_user_management()  # 调用补全后的方法
        tester.test_export_spec()
    finally:
        # 无论成功失败，尝试清理环境
        tester.cleanup()
        tester.print_summary()


if __name__ == "__main__":
    run_all()
