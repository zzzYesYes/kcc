// 租户管理页面逻辑

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', () => {
    loadTenants();
    setupEventListeners();
});

// 加载租户列表
async function loadTenants() {
    try {
        const tenants = await listTenants();
        renderTenantList(tenants);
    } catch (error) {
        showErrorToast('加载租户列表失败: ' + error.message);
    }
}

// 渲染租户列表
function renderTenantList(tenants) {
    const tbody = document.getElementById('tenantListBody');
    if (!tbody) return;

    if (tenants.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="4" class="empty-state">
                    <p>暂无租户</p>
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = tenants.map(tenant => `
        <tr>
            <td><a href="tenant.html?realm=${encodeURIComponent(tenant.realm)}" class="btn-link">${tenant.realm}</a></td>
            <td>${tenant.displayName || '-'}</td>
            <td>${tenant.enabled ? '启用' : '禁用'}</td>
            <td>
                <button class="btn btn-danger" onclick="deleteTenantHandler('${tenant.realm}')">删除</button>
            </td>
        </tr>
    `).join('');
}

// 设置事件监听器
function setupEventListeners() {
    // 创建租户按钮
    const createTenantBtn = document.getElementById('createTenantBtn');
    if (createTenantBtn) {
        createTenantBtn.addEventListener('click', showCreateTenantModal);
    }
}

// 显示创建租户模态框
function showCreateTenantModal() {
    const content = `
        <form id="createTenantForm">
            <div class="form-group">
                <label for="tenantRealm">Realm名称 *</label>
                <input type="text" id="tenantRealm" name="realm" required placeholder="请输入realm名称">
            </div>
            <div class="form-group">
                <label for="tenantDisplayName">显示名称</label>
                <input type="text" id="tenantDisplayName" name="displayName" placeholder="请输入显示名称">
            </div>
        </form>
    `;

    showModal('创建租户', content, async () => {
        const form = document.getElementById('createTenantForm');
        const formData = new FormData(form);
        const tenantData = {
            realm: formData.get('realm'),
            displayName: formData.get('displayName'),
        };

        if (!tenantData.realm) {
            showErrorToast('请输入realm名称');
            return;
        }

        try {
            await createTenant(tenantData);
            showSuccessToast('租户创建成功');
            closeAllModals();
            loadTenants();
        } catch (error) {
            showErrorToast('创建租户失败: ' + error.message);
        }
    });
}

// 删除租户处理函数
function deleteTenantHandler(realmName) {
    showConfirmDialog(`确定要删除租户 "${realmName}" 吗？此操作不可恢复！`, async () => {
        try {
            await deleteTenant(realmName);
            showSuccessToast('租户删除成功');
            loadTenants();
        } catch (error) {
            showErrorToast('删除租户失败: ' + error.message);
        }
    });
}
