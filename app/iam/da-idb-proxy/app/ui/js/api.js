// API基础URL
const API_BASE_URL = '/api/v1';

// 封装fetch API调用
async function apiCall(url, options = {}) {
    const defaultOptions = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const mergedOptions = { ...defaultOptions, ...options };

    try {
        const response = await fetch(url, mergedOptions);

        // 检查响应状态
        if (!response.ok) {
            // 尝试解析错误信息
            let errorMessage = '请求失败';
            try {
                const data = await response.json();
                errorMessage = data.detail || data.message || errorMessage;
            } catch (e) {
                // 如果无法解析JSON，使用默认错误信息
            }
            throw new Error(errorMessage);
        }

        // 处理204 No Content等无响应体的情况
        if (response.status === 204) {
            return null;
        }

        // 尝试解析JSON响应
        const data = await response.json();
        return data;
    } catch (error) {
        throw error;
    }
}

// ==================== 租户管理API ====================

// 获取租户列表
async function listTenants() {
    return apiCall(`${API_BASE_URL}/tenants`);
}

// 创建租户
async function createTenant(tenantData) {
    return apiCall(`${API_BASE_URL}/tenants`, {
        method: 'POST',
        body: JSON.stringify(tenantData),
    });
}

// 删除租户
async function deleteTenant(realmName) {
    return apiCall(`${API_BASE_URL}/tenants/${realmName}`, {
        method: 'DELETE',
    });
}

// ==================== User管理API ====================

// 获取用户列表
async function listUsers(realm) {
    return apiCall(`${API_BASE_URL}/${realm}/users`);
}

// 获取用户详情
async function getUserDetails(realm, userId) {
    return apiCall(`${API_BASE_URL}/${realm}/users/${userId}/details`);
}

// ==================== Group管理API ====================

// 获取组列表
async function listGroups(realm) {
    return apiCall(`${API_BASE_URL}/${realm}/groups`);
}

// 创建组
async function createGroup(realm, groupData) {
    return apiCall(`${API_BASE_URL}/${realm}/groups`, {
        method: 'POST',
        body: JSON.stringify(groupData),
    });
}

// 更新组
async function updateGroup(realm, groupId, groupData) {
    return apiCall(`${API_BASE_URL}/${realm}/groups/${groupId}`, {
        method: 'PUT',
        body: JSON.stringify(groupData),
    });
}

// 删除组
async function deleteGroup(realm, groupId) {
    return apiCall(`${API_BASE_URL}/${realm}/groups/${groupId}`, {
        method: 'DELETE',
    });
}

// 获取组详情（包含成员和角色）
async function getGroupDetail(realm, groupId) {
    return apiCall(`${API_BASE_URL}/${realm}/groups/${groupId}`);
}

// ==================== Role管理API ====================

// 获取角色列表
async function listRoles(realm) {
    return apiCall(`${API_BASE_URL}/${realm}/roles`);
}

// 创建角色
async function createRole(realm, roleData) {
    return apiCall(`${API_BASE_URL}/${realm}/roles`, {
        method: 'POST',
        body: JSON.stringify(roleData),
    });
}

// 获取角色详情
async function getRole(realm, roleName) {
    return apiCall(`${API_BASE_URL}/${realm}/roles/${roleName}`);
}

// 更新角色
async function updateRole(realm, roleName, roleData) {
    return apiCall(`${API_BASE_URL}/${realm}/roles/${roleName}`, {
        method: 'PUT',
        body: JSON.stringify(roleData),
    });
}

// 删除角色
async function deleteRole(realm, roleName) {
    return apiCall(`${API_BASE_URL}/${realm}/roles/${roleName}`, {
        method: 'DELETE',
    });
}

// ==================== IDP管理API ====================

// 获取IDP实例列表
async function listIdpInstances(realm) {
    return apiCall(`${API_BASE_URL}/${realm}/idp/saml/instances`);
}

// 创建IDP实例
async function createIdpInstance(realm, idpData) {
    return apiCall(`${API_BASE_URL}/${realm}/idp/saml/instances`, {
        method: 'POST',
        body: JSON.stringify(idpData),
    });
}

// 更新IDP实例
async function updateIdpInstance(realm, idpData) {
    return apiCall(`${API_BASE_URL}/${realm}/idp/saml/instances`, {
        method: 'PUT',
        body: JSON.stringify(idpData),
    });
}

// 删除IDP实例
async function deleteIdpInstance(realm, alias) {
    return apiCall(`${API_BASE_URL}/${realm}/idp/saml/instances/${alias}`, {
        method: 'DELETE',
    });
}

// 导入SAML metadata
async function importSamlMetadata(realm, formData) {
    return apiCall(`${API_BASE_URL}/${realm}/idp/saml/import`, {
        method: 'POST',
        body: formData,
        headers: {}, // 不设置Content-Type，让浏览器自动设置multipart/form-data
    });
}

// ==================== IDP Mapper管理API ====================

// 获取IDP实例的Mapper列表
async function listIdpMappers(realm, alias) {
    return apiCall(`${API_BASE_URL}/${realm}/idp/saml/instances/${alias}/mappers`);
}

// 创建IDP Mapper
async function createIdpMapper(realm, alias, mapperData) {
    return apiCall(`${API_BASE_URL}/${realm}/idp/saml/instances/${alias}/mappers`, {
        method: 'POST',
        body: JSON.stringify(mapperData),
    });
}

// 更新IDP Mapper
async function updateIdpMapper(realm, alias, mapper_id, mapperData) {
    return apiCall(`${API_BASE_URL}/${realm}/idp/saml/instances/${alias}/mappers/${mapper_id}`, {
        method: 'PUT',
        body: JSON.stringify(mapperData),
    });
}

// 删除IDP Mapper
async function deleteIdpMapper(realm, alias, mapper_id) {
    return apiCall(`${API_BASE_URL}/${realm}/idp/saml/instances/${alias}/mappers/${mapper_id}`, {
        method: 'DELETE',
    });
}

// ==================== UI工具函数 ====================

// 显示成功提示
function showSuccessToast(message) {
    showToast(message, 'success');
}

// 显示错误提示
function showErrorToast(message) {
    showToast(message, 'error');
}

// 显示Toast提示
function showToast(message, type = 'success') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    document.body.appendChild(toast);

    setTimeout(() => {
        toast.remove();
    }, 3000);
}

// 显示确认对话框
function showConfirmDialog(message, onConfirm) {
    const modal = document.createElement('div');
    modal.className = 'modal show';
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>确认操作</h3>
                <button class="modal-close" onclick="this.closest('.modal').remove()">&times;</button>
            </div>
            <div class="modal-body confirm-dialog">
                <p>${message}</p>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="this.closest('.modal').remove()">取消</button>
                <button class="btn btn-danger" id="confirmBtn">确认</button>
            </div>
        </div>
    `;
    document.body.appendChild(modal);

    document.getElementById('confirmBtn').addEventListener('click', () => {
        modal.remove();
        onConfirm();
    });
}

// 显示模态框
function showModal(title, content, onConfirm = null) {
    const modal = document.createElement('div');
    modal.className = 'modal show';
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>${title}</h3>
                <button class="modal-close" onclick="this.closest('.modal').remove()">&times;</button>
            </div>
            <div class="modal-body">
                ${content}
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="this.closest('.modal').remove()">取消</button>
                ${onConfirm ? '<button class="btn btn-primary" id="modalConfirmBtn">确认</button>' : ''}
            </div>
        </div>
    `;
    document.body.appendChild(modal);

    if (onConfirm) {
        document.getElementById('modalConfirmBtn').addEventListener('click', () => {
            onConfirm();
        });
    }

    return modal;
}

// 关闭所有模态框
function closeAllModals() {
    document.querySelectorAll('.modal').forEach(modal => modal.remove());
}

// ==================== 将函数暴露到全局作用域 ====================
window.listTenants = listTenants;
window.createTenant = createTenant;
window.deleteTenant = deleteTenant;
window.listUsers = listUsers;
window.getUserDetails = getUserDetails;
window.listGroups = listGroups;
window.createGroup = createGroup;
window.updateGroup = updateGroup;
window.deleteGroup = deleteGroup;
window.getGroupDetail = getGroupDetail;
window.listRoles = listRoles;
window.createRole = createRole;
window.getRole = getRole;
window.updateRole = updateRole;
window.deleteRole = deleteRole;
window.listIdpInstances = listIdpInstances;
window.createIdpInstance = createIdpInstance;
window.updateIdpInstance = updateIdpInstance;
window.deleteIdpInstance = deleteIdpInstance;
window.importSamlMetadata = importSamlMetadata;
window.listIdpMappers = listIdpMappers;
window.createIdpMapper = createIdpMapper;
window.updateIdpMapper = updateIdpMapper;
window.deleteIdpMapper = deleteIdpMapper;
window.showSuccessToast = showSuccessToast;
window.showErrorToast = showErrorToast;
window.showConfirmDialog = showConfirmDialog;
window.showModal = showModal;
window.closeAllModals = closeAllModals;
