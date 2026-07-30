// 租户内部页面逻辑

let currentRealm = '';
let currentModule = 'user';

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', () => {
    // 从URL参数获取realm
    const urlParams = new URLSearchParams(window.location.search);
    currentRealm = urlParams.get('realm') || '';

    // 初始化租户选择器
    initTenantSelector();

    // 设置事件监听器
    setupEventListeners();

    // 加载当前模块数据
    loadCurrentModule();
});

// 初始化租户选择器
async function initTenantSelector() {
    try {
        const tenants = await listTenants();
        const selector = document.getElementById('tenantSelector');
        if (!selector) return;

        selector.innerHTML = tenants.map(tenant => `
            <option value="${tenant.realm}" ${tenant.realm === currentRealm ? 'selected' : ''}>
                ${tenant.displayName || tenant.realm}
            </option>
        `).join('');

        selector.addEventListener('change', (e) => {
            currentRealm = e.target.value;
            loadCurrentModule();
        });
    } catch (error) {
        showErrorToast('加载租户列表失败: ' + error.message);
    }
}

// 设置事件监听器
function setupEventListeners() {
    // 侧边栏导航
    const navItems = document.querySelectorAll('.sidebar nav ul li');
    navItems.forEach(item => {
        item.addEventListener('click', () => {
            navItems.forEach(i => i.classList.remove('active'));
            item.classList.add('active');
            currentModule = item.dataset.module;
            loadCurrentModule();
        });
    });
}

// 加载当前模块数据
function loadCurrentModule() {
    if (!currentRealm) {
        showErrorToast('请先选择租户');
        return;
    }

    const contentPanel = document.getElementById('contentPanel');
    if (!contentPanel) return;

    switch (currentModule) {
        case 'user':
            loadUserModule();
            break;
        case 'group':
            loadGroupModule();
            break;
        case 'role':
            loadRoleModule();
            break;
        case 'idp':
            loadIdpModule();
            break;
    }
}

// ==================== User模块 ====================

async function loadUserModule() {
    const contentPanel = document.getElementById('contentPanel');
    contentPanel.innerHTML = `
        <div class="page-header">
            <h1>用户管理</h1>
        </div>
        <div id="userListContainer">
            <div class="loading">加载中</div>
        </div>
    `;

    try {
        const users = await listUsers(currentRealm);
        renderUserList(users);
    } catch (error) {
        showErrorToast('加载用户列表失败: ' + error.message);
        document.getElementById('userListContainer').innerHTML = `
            <div class="empty-state">
                <p>加载失败: ${error.message}</p>
            </div>
        `;
    }
}

function renderUserList(users) {
    const container = document.getElementById('userListContainer');
    if (!container) return;

    if (users.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <p>暂无用户</p>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th>用户名</th>
                    <th>邮箱</th>
                    <th>姓名</th>
                    <th>启用状态</th>
                    <th>操作</th>
                </tr>
            </thead>
            <tbody>
                ${users.map(user => `
                    <tr>
                        <td>${user.username || '-'}</td>
                        <td>${user.email || '-'}</td>
                        <td>${user.firstName || ''} ${user.lastName || ''}</td>
                        <td>${user.enabled ? '启用' : '禁用'}</td>
                        <td>
                            <button class="btn btn-link" onclick="viewUser('${user.id}')">查看</button>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

async function viewUser(userId) {
    try {
        const userDetails = await getUserDetails(currentRealm, userId);
        const content = `
            <ul class="detail-list">
                <li><strong>用户名:</strong> ${userDetails.username || '-'}</li>
                <li><strong>邮箱:</strong> ${userDetails.email || '-'}</li>
                <li><strong>姓名:</strong> ${userDetails.firstName || ''} ${userDetails.lastName || ''}</li>
                <li><strong>启用状态:</strong> ${userDetails.enabled ? '启用' : '禁用'}</li>
                <li><strong>所属组:</strong> ${userDetails.groups && userDetails.groups.length > 0 ? userDetails.groups.map(g => g.name).join(', ') : '无'}</li>
                <li><strong>所属角色:</strong> ${userDetails.roles && userDetails.roles.length > 0 ? userDetails.roles.map(r => r.name || r).join(', ') : '无'}</li>
            </ul>
        `;
        showModal('用户详情', content);
    } catch (error) {
        showErrorToast('获取用户详情失败: ' + error.message);
    }
}

// ==================== Group模块 ====================

async function loadGroupModule() {
    const contentPanel = document.getElementById('contentPanel');
    contentPanel.innerHTML = `
        <div class="page-header">
            <h1>组管理</h1>
        </div>
        <div id="groupListContainer">
            <div class="loading">加载中</div>
        </div>
        <div style="margin-top: 20px;">
            <button class="btn btn-primary" onclick="showCreateGroupModal()">添加组</button>
        </div>
    `;

    try {
        const groups = await listGroups(currentRealm);
        renderGroupList(groups);
    } catch (error) {
        showErrorToast('加载组列表失败: ' + error.message);
        document.getElementById('groupListContainer').innerHTML = `
            <div class="empty-state">
                <p>加载失败: ${error.message}</p>
            </div>
        `;
    }
}

function renderGroupList(groups) {
    const container = document.getElementById('groupListContainer');
    if (!container) return;

    if (groups.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <p>暂无组</p>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th><input type="checkbox" id="selectAllGroups" onchange="toggleSelectAllGroups()"></th>
                    <th>名称</th>
                    <th>路径</th>
                    <th>操作</th>
                </tr>
            </thead>
            <tbody>
                ${groups.map(group => `
                    <tr data-group-id="${group.id}">
                        <td><input type="checkbox" class="group-checkbox" value="${group.id}"></td>
                        <td>${group.name || '-'}</td>
                        <td>${group.path || '-'}</td>
                        <td>
                            <button class="btn btn-link" onclick="viewGroup('${group.id}')">查看</button>
                            <button class="btn btn-link" onclick="showEditGroupModal('${group.id}')">编辑</button>
                            <button class="btn btn-link" onclick="deleteGroupHandler('${group.id}')">删除</button>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

function toggleSelectAllGroups() {
    const selectAll = document.getElementById('selectAllGroups');
    const checkboxes = document.querySelectorAll('.group-checkbox');
    checkboxes.forEach(cb => cb.checked = selectAll.checked);
}

async function viewGroup(groupId) {
    try {
        const groupDetail = await getGroupDetail(currentRealm, groupId);
        if (!groupDetail) {
            showErrorToast('未找到该组');
            return;
        }

        const membersHtml = groupDetail.members && groupDetail.members.length > 0
            ? groupDetail.members.map(m => `<li>${m.username} (${m.id})</li>`).join('')
            : '<li>无成员</li>';

        const rolesHtml = groupDetail.roles && groupDetail.roles.length > 0
            ? groupDetail.roles.map(r => `<li>${r.name}</li>`).join('')
            : '<li>无角色</li>';

        const content = `
            <ul class="detail-list">
                <li><strong>名称:</strong> ${groupDetail.name || '-'}</li>
                <li><strong>成员:</strong></li>
                <ul>${membersHtml}</ul>
                <li><strong>角色:</strong></li>
                <ul>${rolesHtml}</ul>
            </ul>
        `;
        showModal('组详情', content);
    } catch (error) {
        showErrorToast('获取组详情失败: ' + error.message);
    }
}

async function showEditGroupModal(groupId) {
    try {
        // 获取组详情（包含成员和角色）
        const groupDetail = await getGroupDetail(currentRealm, groupId);
        if (!groupDetail) {
            showErrorToast('未找到该组');
            return;
        }

        // 获取所有角色和用户
        let roles = [];
        let users = [];
        try {
            roles = await listRoles(currentRealm);
            users = await listUsers(currentRealm);
        } catch (error) {
            showErrorToast('加载角色或用户列表失败: ' + error.message);
            return;
        }

        const content = `
            <form id="editGroupForm">
                <div class="form-group">
                    <label for="groupName">组名称 *</label>
                    <input type="text" id="groupName" name="name" value="${groupDetail.name || ''}" required>
                </div>
                <div class="form-group">
                    <label for="groupPath">路径</label>
                    <input type="text" id="groupPath" name="path" value="${groupDetail.path || ''}">
                </div>
                <div class="form-group">
                    <label for="roleSelect">添加角色</label>
                    <select id="roleSelect">
                        <option value="">请选择角色</option>
                        ${roles.map(r => `<option value="${r.name}">${r.name}</option>`).join('')}
                    </select>
                    <button type="button" class="btn btn-secondary" onclick="addRole()">添加</button>
                    <div id="selectedRoles" class="tag-container"></div>
                </div>
                <div class="form-group">
                    <label for="userSelect">添加用户</label>
                    <select id="userSelect">
                        <option value="">请选择用户</option>
                        ${users.map(u => `<option value="${u.id}">${u.username || u.email || u.id}</option>`).join('')}
                    </select>
                    <button type="button" class="btn btn-secondary" onclick="addUser()">添加</button>
                    <div id="selectedUsers" class="tag-container"></div>
                </div>
            </form>
        `;

        showModal('编辑组', content, async () => {
            const form = document.getElementById('editGroupForm');
            const formData = new FormData(form);
            const groupData = {
                name: formData.get('name'),
                path: formData.get('path'),
                users: window.selectedUsers || [],
                roles: window.selectedRoles || [],
            };

            if (!groupData.name) {
                showErrorToast('请输入组名称');
                return;
            }

            try {
                await updateGroup(currentRealm, groupId, groupData);
                showSuccessToast('组更新成功');
                closeAllModals();
                loadGroupModule();
            } catch (error) {
                showErrorToast('更新组失败: ' + error.message);
            }
        });

        // 初始化选中的角色和用户
        window.selectedRoles = groupDetail.roles ? groupDetail.roles.map(r => r.name) : [];
        window.selectedUsers = groupDetail.members ? groupDetail.members.map(m => m.id) : [];
        renderSelectedRoles();
        renderSelectedUsers();
    } catch (error) {
        showErrorToast('加载组信息失败: ' + error.message);
    }
}

async function showCreateGroupModal() {
    // 获取所有角色和用户
    let roles = [];
    let users = [];
    try {
        roles = await listRoles(currentRealm);
        users = await listUsers(currentRealm);
    } catch (error) {
        showErrorToast('加载角色或用户列表失败: ' + error.message);
        return;
    }

    const content = `
        <form id="createGroupForm">
            <div class="form-group">
                <label for="groupName">组名称 *</label>
                <input type="text" id="groupName" name="name" required placeholder="请输入组名称">
            </div>
            <div class="form-group">
                <label for="groupPath">路径</label>
                <input type="text" id="groupPath" name="path" placeholder="请输入路径">
            </div>
            <div class="form-group">
                <label for="roleSelect">添加角色</label>
                <select id="roleSelect">
                    <option value="">请选择角色</option>
                    ${roles.map(r => `<option value="${r.name}">${r.name}</option>`).join('')}
                </select>
                <button type="button" class="btn btn-secondary" onclick="addRole()">添加</button>
                <div id="selectedRoles" class="tag-container"></div>
            </div>
            <div class="form-group">
                <label for="userSelect">添加用户</label>
                <select id="userSelect">
                    <option value="">请选择用户</option>
                    ${users.map(u => `<option value="${u.id}">${u.username || u.email || u.id}</option>`).join('')}
                </select>
                <button type="button" class="btn btn-secondary" onclick="addUser()">添加</button>
                <div id="selectedUsers" class="tag-container"></div>
            </div>
        </form>
    `;

    showModal('创建组', content, async () => {
        const form = document.getElementById('createGroupForm');
        const formData = new FormData(form);
        const groupData = {
            name: formData.get('name'),
            path: formData.get('path'),
            users: window.selectedUsers || [],
            roles: window.selectedRoles || [],
        };

        if (!groupData.name) {
            showErrorToast('请输入组名称');
            return;
        }

        try {
            await createGroup(currentRealm, groupData);
            showSuccessToast('组创建成功');
            closeAllModals();
            loadGroupModule();
        } catch (error) {
            showErrorToast('创建组失败: ' + error.message);
        }
    });

    // 初始化选中的角色和用户
    window.selectedRoles = [];
    window.selectedUsers = [];
    renderSelectedRoles();
    renderSelectedUsers();
}

function addRole() {
    const select = document.getElementById('roleSelect');
    const roleName = select.value;
    if (!roleName) {
        showErrorToast('请选择角色');
        return;
    }
    if (window.selectedRoles.includes(roleName)) {
        showErrorToast('该角色已添加');
        return;
    }
    window.selectedRoles.push(roleName);
    renderSelectedRoles();
    select.value = '';
}

function removeRole(roleName) {
    window.selectedRoles = window.selectedRoles.filter(r => r !== roleName);
    renderSelectedRoles();
}

function renderSelectedRoles() {
    const container = document.getElementById('selectedRoles');
    if (!container) return;
    container.innerHTML = window.selectedRoles.map(role => `
        <span class="tag">
            ${role}
            <span class="tag-close" onclick="removeRole('${role}')">&times;</span>
        </span>
    `).join('');
}

function addUser() {
    const select = document.getElementById('userSelect');
    const userId = select.value;
    if (!userId) {
        showErrorToast('请选择用户');
        return;
    }
    if (window.selectedUsers.includes(userId)) {
        showErrorToast('该用户已添加');
        return;
    }
    window.selectedUsers.push(userId);
    renderSelectedUsers();
    select.value = '';
}

function removeUser(userId) {
    window.selectedUsers = window.selectedUsers.filter(u => u !== userId);
    renderSelectedUsers();
}

function renderSelectedUsers() {
    const container = document.getElementById('selectedUsers');
    if (!container) return;
    const select = document.getElementById('userSelect');
    const options = select ? Array.from(select.options) : [];
    container.innerHTML = window.selectedUsers.map(userId => {
        const option = options.find(opt => opt.value === userId);
        const label = option ? option.text : userId;
        return `
            <span class="tag">
                ${label}
                <span class="tag-close" onclick="removeUser('${userId}')">&times;</span>
            </span>
        `;
    }).join('');
}

function deleteGroupHandler(groupId) {
    showConfirmDialog('确定要删除该组吗？', async () => {
        try {
            await deleteGroup(currentRealm, groupId);
            showSuccessToast('组删除成功');
            loadGroupModule();
        } catch (error) {
            showErrorToast('删除组失败: ' + error.message);
        }
    });
}

// ==================== Role模块 ====================

async function loadRoleModule() {
    const contentPanel = document.getElementById('contentPanel');
    contentPanel.innerHTML = `
        <div class="page-header">
            <h1>角色管理</h1>
        </div>
        <div id="roleListContainer">
            <div class="loading">加载中</div>
        </div>
        <div style="margin-top: 20px;">
            <button class="btn btn-primary" onclick="showCreateRoleModal()">添加角色</button>
        </div>
    `;

    try {
        const roles = await listRoles(currentRealm);
        renderRoleList(roles);
    } catch (error) {
        showErrorToast('加载角色列表失败: ' + error.message);
        document.getElementById('roleListContainer').innerHTML = `
            <div class="empty-state">
                <p>加载失败: ${error.message}</p>
            </div>
        `;
    }
}

function renderRoleList(roles) {
    const container = document.getElementById('roleListContainer');
    if (!container) return;

    if (roles.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <p>暂无角色</p>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th><input type="checkbox" id="selectAllRoles" onchange="toggleSelectAllRoles()"></th>
                    <th>名称</th>
                    <th>描述</th>
                    <th>绑定策略</th>
                    <th>操作</th>
                </tr>
            </thead>
            <tbody>
                ${roles.map(role => {
                    const policy = role.policy;
                    const policyDisplay = policy ? policy.id : '-';
                    const policyClass = policy ? 'policy-tag' : '';
                    return `
                    <tr data-role-name="${role.name}">
                        <td><input type="checkbox" class="role-checkbox" value="${role.name}"></td>
                        <td>${role.name || '-'}</td>
                        <td>${role.description || '-'}</td>
                        <td>${policyDisplay ? `<span class="${policyClass}">${policyDisplay}</span>` : '-'}</td>
                        <td>
                            <button class="btn btn-link" onclick="viewRole('${role.name}')">查看</button>
                            <button class="btn btn-link" onclick="showEditRoleModal('${role.name}')">编辑</button>
                            <button class="btn btn-link" onclick="deleteRoleHandler('${role.name}')">删除</button>
                        </td>
                    </tr>
                `;}).join('')}
            </tbody>
        </table>
    `;
}

function toggleSelectAllRoles() {
    const selectAll = document.getElementById('selectAllRoles');
    const checkboxes = document.querySelectorAll('.role-checkbox');
    checkboxes.forEach(cb => cb.checked = selectAll.checked);
}

async function viewRole(roleName) {
    try {
        const role = await getRole(currentRealm, roleName);

        let policyHtml = '<li><strong>绑定策略:</strong> 无</li>';

        if (role.policy) {
            const policy = role.policy;
            const rulesPreview = policy.rules && policy.rules.length > 0
                ? policy.rules.slice(0, 3).map((r, i) => `<div style="font-size: 0.9em; margin-left: 10px; color: #666;">${i + 1}. ${JSON.stringify(r).substring(0, 50)}${JSON.stringify(r).length > 50 ? '...' : ''}</div>`).join('')
                : '<div style="font-size: 0.9em; margin-left: 10px; color: #666;">无规则</div>';

            policyHtml = `
                <li>
                    <strong>绑定策略:</strong>
                    <div class="policy-card" style="margin-left: 10px; background: #f5f7fa; padding: 10px; border-radius: 4px; border-left: 3px solid #4CAF50;">
                        <div style="margin-bottom: 5px;">
                            <strong>策略ID:</strong> <span class="policy-id">${policy.id || '-'}</span>
                        </div>
                        <div style="margin-bottom: 5px;">
                            <strong>租户ID:</strong> ${policy.tenantId || '-'}
                        </div>
                        <div style="margin-bottom: 5px;">
                            <strong>创建时间:</strong> ${policy.createdAt || '-'}
                        </div>
                        <div style="margin-bottom: 5px;">
                            <strong>更新时间:</strong> ${policy.updatedAt || '-'}
                        </div>
                        <div>
                            <strong>规则:</strong>
                            ${rulesPreview}
                        </div>
                    </div>
                </li>
            `;
        }

        const content = `
            <ul class="detail-list">
                <li><strong>名称:</strong> ${role.name || '-'}</li>
                <li><strong>描述:</strong> ${role.description || '-'}</li>
                ${policyHtml}
            </ul>
        `;
        showModal('角色详情', content);
    } catch (error) {
        showErrorToast('获取角色详情失败: ' + error.message);
    }
}

async function showEditRoleModal(roleName) {
    try {
        const role = await getRole(currentRealm, roleName);
        const currentPolicyId = role.policy ? role.policy.id : '';
        const hasPolicy = !!role.policy;

        const content = `
            <form id="editRoleForm">
                <div class="form-group">
                    <label for="roleName">角色名称 *</label>
                    <input type="text" id="roleName" name="name" value="${role.name || ''}" required>
                </div>
                <div class="form-group">
                    <label for="roleDescription">描述</label>
                    <textarea id="roleDescription" name="description">${role.description || ''}</textarea>
                </div>
                <div class="form-group">
                    <label for="policyId">绑定策略ID</label>
                    <div style="display: flex; gap: 10px; align-items: flex-start;">
                        <input type="text" id="policyId" name="policy_id" value="${currentPolicyId}" placeholder="请输入策略ID（可留空解绑）" style="flex: 1;">
                        ${hasPolicy ? `<button type="button" class="btn btn-danger" id="unbindBtn" style="white-space: nowrap;">解绑策略</button>` : ''}
                    </div>
                    <small class="form-text">留空则解绑当前策略</small>
                </div>
            </form>
        `;

        showModal('编辑角色', content, async () => {
            const form = document.getElementById('editRoleForm');
            const formData = new FormData(form);
            const roleData = {
                name: formData.get('name'),
                description: formData.get('description'),
                policy_id: formData.get('policy_id') || null
            };

            if (!roleData.name) {
                showErrorToast('请输入角色名称');
                return;
            }

            try {
                await updateRole(currentRealm, roleName, roleData);
                showSuccessToast('角色更新成功');
                closeAllModals();
                loadRoleModule();
            } catch (error) {
                showErrorToast('更新角色失败: ' + error.message);
            }
        });

        if (hasPolicy) {
            const unbindBtn = document.getElementById('unbindBtn');
            const policyInput = document.getElementById('policyId');
            unbindBtn.addEventListener('click', () => {
                policyInput.value = '';
                policyInput.placeholder = '策略已解绑';
            });
        }
    } catch (error) {
        showErrorToast('加载角色信息失败: ' + error.message);
    }
}

function showCreateRoleModal() {
    const content = `
        <form id="createRoleForm">
            <div class="form-group">
                <label for="roleName">角色名称 *</label>
                <input type="text" id="roleName" name="name" required placeholder="请输入角色名称">
            </div>
            <div class="form-group">
                <label for="roleDescription">描述</label>
                <textarea id="roleDescription" name="description" placeholder="请输入描述"></textarea>
            </div>
            <div class="form-group">
                <label for="policyId">绑定策略ID</label>
                <input type="text" id="policyId" name="policy_id" placeholder="请输入策略ID（可选）">
                <small class="form-text">可选，如需绑定策略请在此输入策略ID</small>
            </div>
        </form>
    `;

    showModal('创建角色', content, async () => {
        const form = document.getElementById('createRoleForm');
        const formData = new FormData(form);
        const roleData = {
            name: formData.get('name'),
            description: formData.get('description'),
            policy_id: formData.get('policy_id') || null
        };

        if (!roleData.name) {
            showErrorToast('请输入角色名称');
            return;
        }

        try {
            await createRole(currentRealm, roleData);
            showSuccessToast('角色创建成功');
            closeAllModals();
            loadRoleModule();
        } catch (error) {
            showErrorToast('创建角色失败: ' + error.message);
        }
    });
}

function deleteRoleHandler(roleName) {
    showConfirmDialog(`确定要删除角色 "${roleName}" 吗？`, async () => {
        try {
            await deleteRole(currentRealm, roleName);
            showSuccessToast('角色删除成功');
            loadRoleModule();
        } catch (error) {
            showErrorToast('删除角色失败: ' + error.message);
        }
    });
}

// ==================== IDP模块 ====================

function deleteMapperHandler(idpAlias, mapperId) {
    showConfirmDialog('确定要删除该Mapper吗？', async () => {
        try {
            await deleteIdpMapper(currentRealm, idpAlias, mapperId);
            showSuccessToast('Mapper删除成功');
            loadMappersForIdp(idpAlias);
        } catch (error) {
            showErrorToast('删除Mapper失败: ' + error.message);
        }
    });
}

function showCreateMapperModal(idpAlias, onCreated) {
    const content = `
        <form id="createMapperForm">
            <div class="form-group">
                <label for="mapperName">Mapper名称 *</label>
                <input type="text" id="mapperName" name="name" required placeholder="请输入Mapper名称">
            </div>
            <div class="form-group">
                <label for="mapperAttributeKey">属性键（Remote Attribute）*</label>
                <input type="text" id="mapperAttributeKey" name="attributeKey" required placeholder="SAML属性名，如 email">
                <small class="form-text">SAML断言中的属性名称</small>
            </div>
            <div class="form-group">
                <label for="mapperAttributeValue">属性值（Local Attribute）*</label>
                <input type="text" id="mapperAttributeValue" name="attributeValue" required placeholder="Keycloak用户属性名，如 email">
                <small class="form-text">映射到Keycloak用户的属性名称</small>
            </div>
            <div class="form-group">
                <label for="mapperFriendlyName">友好名称</label>
                <input type="text" id="mapperFriendlyName" name="friendlyName" placeholder="可选，用于显示">
                <small class="form-text">可选，用于界面显示</small>
            </div>
        </form>
    `;

    showModal('创建Mapper', content, async () => {
        const form = document.getElementById('createMapperForm');
        const formData = new FormData(form);
        const mapperData = {
            name: formData.get('name'),
            attributeKey: formData.get('attributeKey'),
            attributeValue: formData.get('attributeValue'),
            friendlyName: formData.get('friendlyName') || null
        };

        if (!mapperData.name || !mapperData.attributeKey || !mapperData.attributeValue) {
            showErrorToast('请填写所有必填字段');
            return;
        }

        try {
            await createIdpMapper(currentRealm, idpAlias, mapperData);
            showSuccessToast('Mapper创建成功');
            closeAllModals();
            if (onCreated) {
                onCreated();
            }
        } catch (error) {
            showErrorToast('创建Mapper失败: ' + error.message);
        }
    });
}

async function loadIdpModule() {
    const contentPanel = document.getElementById('contentPanel');
    contentPanel.innerHTML = `
        <div class="page-header">
            <h1>ID Provider管理</h1>
        </div>
        <div id="idpListContainer">
            <div class="loading">加载中</div>
        </div>
        <div style="margin-top: 20px;">
            <button class="btn btn-primary" onclick="showCreateIdpModal()">添加IDP</button>
        </div>
    `;

    try {
        const idps = await listIdpInstances(currentRealm);
        renderIdpList(idps);
    } catch (error) {
        showErrorToast('加载IDP列表失败: ' + error.message);
        document.getElementById('idpListContainer').innerHTML = `
            <div class="empty-state">
                <p>加载失败: ${error.message}</p>
            </div>
        `;
    }
}

function renderIdpList(idps) {
    const container = document.getElementById('idpListContainer');
    if (!container) return;

    if (idps.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <p>暂无IDP实例</p>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th>别名</th>
                    <th>显示名称</th>
                    <th>启用状态</th>
                    <th>操作</th>
                </tr>
            </thead>
            <tbody>
                ${idps.map(idp => `
                    <tr>
                        <td>${idp.alias || '-'}</td>
                        <td>${idp.displayName || '-'}</td>
                        <td>${idp.enabled ? '启用' : '禁用'}</td>
                        <td>
                            <button class="btn btn-link" onclick="showEditIdpModal('${idp.alias}')">编辑</button>
                            <button class="btn btn-link" onclick="deleteIdpHandler('${idp.alias}')">删除</button>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

function showCreateIdpModal() {
    const content = `
        <form id="createIdpForm">
            <!-- 基础配置 -->
            <div class="form-group">
                <label for="idpAlias">别名 *</label>
                <input type="text" id="idpAlias" name="alias" required placeholder="请输入IDP别名">
            </div>
            <div class="form-group">
                <label for="idpDisplayName">显示名称</label>
                <input type="text" id="idpDisplayName" name="displayName" placeholder="请输入显示名称">
            </div>
            <div class="form-group">
                <label for="idpEnabled">启用状态</label>
                <select id="idpEnabled" name="enabled">
                    <option value="true">启用</option>
                    <option value="false">禁用</option>
                </select>
            </div>
            <div class="form-group">
                <label for="idpTrustEmail">信任邮箱</label>
                <select id="idpTrustEmail" name="trustEmail">
                    <option value="false">不信任</option>
                    <option value="true">信任</option>
                </select>
            </div>

            <!-- 元数据表单（默认折叠） -->
            <div class="collapsible-section">
                <div class="collapsible-header collapsed" id="metadataHeader">
                    <span>SAML元数据配置</span>
                    <span class="toggle-icon">▼</span>
                </div>
                <div class="collapsible-content collapsed" id="metadataContent">
                    <div class="form-group">
                        <label for="samlFile">导入SAML Metadata</label>
                        <div class="file-upload" onclick="document.getElementById('samlFile').click()">
                            <input type="file" id="samlFile" name="file" accept=".xml">
                            <span class="file-upload-label" id="fileLabel">点击选择文件</span>
                        </div>
                        <button type="button" class="btn btn-secondary" id="importBtn" style="margin-top: 10px;">导入并解析</button>
                    </div>
                    <div class="form-group">
                        <label for="idpEntityId">Entity ID</label>
                        <input type="text" id="idpEntityId" name="entityId" placeholder="请输入Entity ID">
                    </div>
                    <div class="form-group">
                        <label for="idpSsoUrl">SSO Service URL *</label>
                        <input type="text" id="idpSsoUrl" name="ssoUrl" required placeholder="请输入SSO Service URL">
                    </div>
                    <div class="form-group">
                        <label for="idpSingleLogoutServiceUrl">Single Logout Service URL</label>
                        <input type="text" id="idpSingleLogoutServiceUrl" name="singleLogoutServiceUrl" placeholder="请输入Single Logout Service URL">
                    </div>
                    <div class="form-group">
                        <label for="idpValidateSignature">Validate Signature</label>
                        <select id="idpValidateSignature" name="validateSignature">
                            <option value="false">否</option>
                            <option value="true">是</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label for="idpSigningCertificate">Signing Certificate</label>
                        <textarea id="idpSigningCertificate" name="signingCertificate" rows="3" placeholder="请输入Signing Certificate"></textarea>
                    </div>
                    <div class="form-group">
                        <label for="idpPostBindingLogout">Post Binding Logout</label>
                        <select id="idpPostBindingLogout" name="postBindingLogout">
                            <option value="false">否</option>
                            <option value="true">是</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label for="idpNameIDPolicyFormat">Name ID Policy Format</label>
                        <input type="text" id="idpNameIDPolicyFormat" name="nameIDPolicyFormat" placeholder="请输入Name ID Policy Format">
                    </div>
                    <div class="form-group">
                        <label for="idpLoginHint">Login Hint</label>
                        <input type="text" id="idpLoginHint" name="loginHint" placeholder="请输入Login Hint">
                    </div>
                    <div class="form-group">
                        <label for="idpMetadataDescriptorUrl">Metadata Descriptor URL</label>
                        <input type="text" id="idpMetadataDescriptorUrl" name="metadataDescriptorUrl" placeholder="请输入Metadata Descriptor URL">
                    </div>
                    <div class="form-group">
                        <label for="idpEnableFormMetadata">Enable Form Metadata</label>
                        <select id="idpEnableFormMetadata" name="enableFormMetadata">
                            <option value="false">否</option>
                            <option value="true">是</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label for="idpPostBindingAuthnRequest">Post Binding Authn Request</label>
                        <select id="idpPostBindingAuthnRequest" name="postBindingAuthnRequest">
                            <option value="false">否</option>
                            <option value="true">是</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label for="idpWantAuthnRequestsSigned">Want Authn Requests Signed</label>
                        <select id="idpWantAuthnRequestsSigned" name="wantAuthnRequestsSigned">
                            <option value="false">否</option>
                            <option value="true">是</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label for="idpArtifactResolutionServiceUrl">Artifact Resolution Service URL</label>
                        <input type="text" id="idpArtifactResolutionServiceUrl" name="artifactResolutionServiceUrl" placeholder="请输入Artifact Resolution Service URL">
                    </div>
                    <div class="form-group">
                        <label for="idpAddExtensionsElementWithKeyInfo">Add Extensions Element With KeyInfo</label>
                        <select id="idpAddExtensionsElementWithKeyInfo" name="addExtensionsElementWithKeyInfo">
                            <option value="false">否</option>
                            <option value="true">是</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label for="idpArtifactBindingResponse">Artifact Binding Response</label>
                        <select id="idpArtifactBindingResponse" name="artifactBindingResponse">
                            <option value="false">否</option>
                            <option value="true">是</option>
                        </select>
                    </div>
                </div>
            </div>
        </form>
    `;

    showModal('创建IDP实例', content, async () => {
        const form = document.getElementById('createIdpForm');
        const formData = new FormData(form);
        const idpData = {
            alias: formData.get('alias'),
            displayName: formData.get('displayName'),
            enabled: formData.get('enabled') === 'true',
            trustEmail: formData.get('trustEmail') === 'true',
            config: {
                entityId: formData.get('entityId'),
                singleSignOnServiceUrl: formData.get('ssoUrl'),
                singleLogoutServiceUrl: formData.get('singleLogoutServiceUrl'),
                validateSignature: formData.get('validateSignature') === 'true',
                signingCertificate: formData.get('signingCertificate'),
                postBindingLogout: formData.get('postBindingLogout') === 'true',
                nameIDPolicyFormat: formData.get('nameIDPolicyFormat'),
                loginHint: formData.get('loginHint'),
                metadataDescriptorUrl: formData.get('metadataDescriptorUrl'),
                enableFormMetadata: formData.get('enableFormMetadata') === 'true',
                postBindingAuthnRequest: formData.get('postBindingAuthnRequest') === 'true',
                wantAuthnRequestsSigned: formData.get('wantAuthnRequestsSigned') === 'true',
                artifactResolutionServiceUrl: formData.get('artifactResolutionServiceUrl'),
                addExtensionsElementWithKeyInfo: formData.get('addExtensionsElementWithKeyInfo') === 'true',
                artifactBindingResponse: formData.get('artifactBindingResponse') === 'true',
            },
        };

        if (!idpData.alias) {
            showErrorToast('请输入IDP别名');
            return;
        }

        if (!idpData.config.singleSignOnServiceUrl) {
            showErrorToast('请输入SSO Service URL');
            return;
        }

        try {
            await createIdpInstance(currentRealm, idpData);
            showSuccessToast('IDP实例创建成功');
            closeAllModals();
            loadIdpModule();
        } catch (error) {
            showErrorToast('创建IDP实例失败: ' + error.message);
        }
    });

    // 折叠面板切换
    const metadataHeader = document.getElementById('metadataHeader');
    const metadataContent = document.getElementById('metadataContent');
    metadataHeader.addEventListener('click', () => {
        metadataHeader.classList.toggle('collapsed');
        metadataContent.classList.toggle('collapsed');
    });

    // 文件选择后更新显示
    document.getElementById('samlFile').addEventListener('change', (e) => {
        const fileLabel = document.getElementById('fileLabel');
        if (e.target.files.length > 0) {
            fileLabel.textContent = e.target.files[0].name;
        }
    });

    // 导入并解析SAML Metadata
    document.getElementById('importBtn').addEventListener('click', async () => {
        const fileInput = document.getElementById('samlFile');
        if (!fileInput.files || fileInput.files.length === 0) {
            showErrorToast('请先选择SAML Metadata文件');
            return;
        }

        const formData = new FormData();
        formData.append('file', fileInput.files[0]);

        try {
            const result = await importSamlMetadata(currentRealm, formData);
            showSuccessToast('SAML Metadata解析成功');
            
            // 回填解析结果到表单
            // 支持两种数据结构：result.config 或直接在result中
            const config = result && result.config ? result.config : result;
            
            if (config) {
                // 回填基础配置
                if (config.alias) {
                    document.getElementById('idpAlias').value = config.alias;
                }
                if (config.displayName) {
                    document.getElementById('idpDisplayName').value = config.displayName;
                }
                
                // 回填元数据配置
                if (config.entityId) {
                    document.getElementById('idpEntityId').value = config.entityId;
                }
                if (config.singleSignOnServiceUrl) {
                    document.getElementById('idpSsoUrl').value = config.singleSignOnServiceUrl;
                }
                if (config.singleLogoutServiceUrl) {
                    document.getElementById('idpSingleLogoutServiceUrl').value = config.singleLogoutServiceUrl;
                }
                if (config.validateSignature !== undefined) {
                    document.getElementById('idpValidateSignature').value = config.validateSignature.toString();
                }
                if (config.signingCertificate) {
                    document.getElementById('idpSigningCertificate').value = config.signingCertificate;
                }
                if (config.postBindingLogout !== undefined) {
                    document.getElementById('idpPostBindingLogout').value = config.postBindingLogout.toString();
                }
                if (config.nameIDPolicyFormat) {
                    document.getElementById('idpNameIDPolicyFormat').value = config.nameIDPolicyFormat;
                }
                if (config.loginHint) {
                    document.getElementById('idpLoginHint').value = config.loginHint;
                }
                if (config.metadataDescriptorUrl) {
                    document.getElementById('idpMetadataDescriptorUrl').value = config.metadataDescriptorUrl;
                }
                if (config.enableFormMetadata !== undefined) {
                    document.getElementById('idpEnableFormMetadata').value = config.enableFormMetadata.toString();
                }
                if (config.postBindingAuthnRequest !== undefined) {
                    document.getElementById('idpPostBindingAuthnRequest').value = config.postBindingAuthnRequest.toString();
                }
                if (config.wantAuthnRequestsSigned !== undefined) {
                    document.getElementById('idpWantAuthnRequestsSigned').value = config.wantAuthnRequestsSigned.toString();
                }
                if (config.artifactResolutionServiceUrl) {
                    document.getElementById('idpArtifactResolutionServiceUrl').value = config.artifactResolutionServiceUrl;
                }
                if (config.addExtensionsElementWithKeyInfo !== undefined) {
                    document.getElementById('idpAddExtensionsElementWithKeyInfo').value = config.addExtensionsElementWithKeyInfo.toString();
                }
                if (config.artifactBindingResponse !== undefined) {
                    document.getElementById('idpArtifactBindingResponse').value = config.artifactBindingResponse.toString();
                }
            }
        } catch (error) {
            showErrorToast('解析SAML Metadata失败: ' + error.message);
        }
    });
}

async function showEditIdpModal(alias) {
    try {
        const idps = await listIdpInstances(currentRealm);
        const idp = idps.find(i => i.alias === alias);
        if (!idp) {
            showErrorToast('未找到该IDP实例');
            return;
        }

        const content = `
            <form id="editIdpForm">
                <!-- 基础配置 -->
                <div class="form-group">
                    <label for="idpAlias">别名 *</label>
                    <input type="text" id="idpAlias" name="alias" value="${idp.alias || ''}" required>
                </div>
                <div class="form-group">
                    <label for="idpDisplayName">显示名称</label>
                    <input type="text" id="idpDisplayName" name="displayName" value="${idp.displayName || ''}">
                </div>
                <div class="form-group">
                    <label for="idpEnabled">启用状态</label>
                    <select id="idpEnabled" name="enabled">
                        <option value="true" ${idp.enabled ? 'selected' : ''}>启用</option>
                        <option value="false" ${!idp.enabled ? 'selected' : ''}>禁用</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="idpTrustEmail">信任邮箱</label>
                    <select id="idpTrustEmail" name="trustEmail">
                        <option value="false" ${!idp.trustEmail ? 'selected' : ''}>不信任</option>
                        <option value="true" ${idp.trustEmail ? 'selected' : ''}>信任</option>
                    </select>
                </div>

                <!-- 元数据表单（默认折叠） -->
                <div class="collapsible-section">
                    <div class="collapsible-header collapsed" id="metadataHeader">
                        <span>SAML元数据配置</span>
                        <span class="toggle-icon">▼</span>
                    </div>
                    <div class="collapsible-content collapsed" id="metadataContent">
                        <div class="form-group">
                            <label for="samlFile">导入SAML Metadata</label>
                            <div class="file-upload" onclick="document.getElementById('samlFile').click()">
                                <input type="file" id="samlFile" name="file" accept=".xml">
                                <span class="file-upload-label" id="fileLabel">点击选择文件</span>
                            </div>
                            <button type="button" class="btn btn-secondary" id="importBtn" style="margin-top: 10px;">导入并解析</button>
                        </div>
                        <div class="form-group">
                            <label for="idpEntityId">Entity ID</label>
                            <input type="text" id="idpEntityId" name="entityId" value="${idp.config?.entityId || ''}">
                        </div>
                        <div class="form-group">
                            <label for="idpSsoUrl">SSO Service URL *</label>
                            <input type="text" id="idpSsoUrl" name="ssoUrl" value="${idp.config?.singleSignOnServiceUrl || ''}" required>
                        </div>
                        <div class="form-group">
                            <label for="idpSingleLogoutServiceUrl">Single Logout Service URL</label>
                            <input type="text" id="idpSingleLogoutServiceUrl" name="singleLogoutServiceUrl" value="${idp.config?.singleLogoutServiceUrl || ''}">
                        </div>
                        <div class="form-group">
                            <label for="idpValidateSignature">Validate Signature</label>
                            <select id="idpValidateSignature" name="validateSignature">
                                <option value="false" ${!idp.config?.validateSignature ? 'selected' : ''}>否</option>
                                <option value="true" ${idp.config?.validateSignature ? 'selected' : ''}>是</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label for="idpSigningCertificate">Signing Certificate</label>
                            <textarea id="idpSigningCertificate" name="signingCertificate" rows="3" placeholder="请输入Signing Certificate">${idp.config?.signingCertificate || ''}</textarea>
                        </div>
                        <div class="form-group">
                            <label for="idpPostBindingLogout">Post Binding Logout</label>
                            <select id="idpPostBindingLogout" name="postBindingLogout">
                                <option value="false" ${!idp.config?.postBindingLogout ? 'selected' : ''}>否</option>
                                <option value="true" ${idp.config?.postBindingLogout ? 'selected' : ''}>是</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label for="idpNameIDPolicyFormat">Name ID Policy Format</label>
                            <input type="text" id="idpNameIDPolicyFormat" name="nameIDPolicyFormat" value="${idp.config?.nameIDPolicyFormat || ''}">
                        </div>
                        <div class="form-group">
                            <label for="idpLoginHint">Login Hint</label>
                            <input type="text" id="idpLoginHint" name="loginHint" value="${idp.config?.loginHint || ''}">
                        </div>
                        <div class="form-group">
                            <label for="idpMetadataDescriptorUrl">Metadata Descriptor URL</label>
                            <input type="text" id="idpMetadataDescriptorUrl" name="metadataDescriptorUrl" value="${idp.config?.metadataDescriptorUrl || ''}">
                        </div>
                        <div class="form-group">
                            <label for="idpEnableFormMetadata">Enable Form Metadata</label>
                            <select id="idpEnableFormMetadata" name="enableFormMetadata">
                                <option value="false" ${!idp.config?.enableFormMetadata ? 'selected' : ''}>否</option>
                                <option value="true" ${idp.config?.enableFormMetadata ? 'selected' : ''}>是</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label for="idpPostBindingAuthnRequest">Post Binding Authn Request</label>
                            <select id="idpPostBindingAuthnRequest" name="postBindingAuthnRequest">
                                <option value="false" ${!idp.config?.postBindingAuthnRequest ? 'selected' : ''}>否</option>
                                <option value="true" ${idp.config?.postBindingAuthnRequest ? 'selected' : ''}>是</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label for="idpWantAuthnRequestsSigned">Want Authn Requests Signed</label>
                            <select id="idpWantAuthnRequestsSigned" name="wantAuthnRequestsSigned">
                                <option value="false" ${!idp.config?.wantAuthnRequestsSigned ? 'selected' : ''}>否</option>
                                <option value="true" ${idp.config?.wantAuthnRequestsSigned ? 'selected' : ''}>是</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label for="idpArtifactResolutionServiceUrl">Artifact Resolution Service URL</label>
                            <input type="text" id="idpArtifactResolutionServiceUrl" name="artifactResolutionServiceUrl" value="${idp.config?.artifactResolutionServiceUrl || ''}">
                        </div>
                        <div class="form-group">
                            <label for="idpAddExtensionsElementWithKeyInfo">Add Extensions Element With KeyInfo</label>
                            <select id="idpAddExtensionsElementWithKeyInfo" name="addExtensionsElementWithKeyInfo">
                                <option value="false" ${!idp.config?.addExtensionsElementWithKeyInfo ? 'selected' : ''}>否</option>
                                <option value="true" ${idp.config?.addExtensionsElementWithKeyInfo ? 'selected' : ''}>是</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label for="idpArtifactBindingResponse">Artifact Binding Response</label>
                            <select id="idpArtifactBindingResponse" name="artifactBindingResponse">
                                <option value="false" ${!idp.config?.artifactBindingResponse ? 'selected' : ''}>否</option>
                                <option value="true" ${idp.config?.artifactBindingResponse ? 'selected' : ''}>是</option>
                            </select>
                        </div>
                    </div>
                </div>

                <!-- Mapper管理（默认折叠） -->
                <div class="collapsible-section">
                    <div class="collapsible-header collapsed" id="mapperHeader">
                        <span>Mapper管理</span>
                        <span class="toggle-icon">▶</span>
                    </div>
                    <div class="collapsible-content collapsed" id="mapperContent">
                        <div style="margin-bottom: 15px;">
                            <button type="button" class="btn btn-primary" id="showCreateMapperModalBtn">添加Mapper</button>
                        </div>
                        <div id="mapperListContainer">
                            <div class="loading">加载中</div>
                        </div>
                    </div>
                </div>
            </form>
        `;

        showModal('编辑IDP实例', content, async () => {
            const form = document.getElementById('editIdpForm');
            const formData = new FormData(form);
            const idpData = {
                alias: formData.get('alias'),
                displayName: formData.get('displayName'),
                enabled: formData.get('enabled') === 'true',
                trustEmail: formData.get('trustEmail') === 'true',
                config: {
                    entityId: formData.get('entityId'),
                    singleSignOnServiceUrl: formData.get('ssoUrl'),
                    singleLogoutServiceUrl: formData.get('singleLogoutServiceUrl'),
                    validateSignature: formData.get('validateSignature') === 'true',
                    signingCertificate: formData.get('signingCertificate'),
                    postBindingLogout: formData.get('postBindingLogout') === 'true',
                    nameIDPolicyFormat: formData.get('nameIDPolicyFormat'),
                    loginHint: formData.get('loginHint'),
                    metadataDescriptorUrl: formData.get('metadataDescriptorUrl'),
                    enableFormMetadata: formData.get('enableFormMetadata') === 'true',
                    postBindingAuthnRequest: formData.get('postBindingAuthnRequest') === 'true',
                    wantAuthnRequestsSigned: formData.get('wantAuthnRequestsSigned') === 'true',
                    artifactResolutionServiceUrl: formData.get('artifactResolutionServiceUrl'),
                    addExtensionsElementWithKeyInfo: formData.get('addExtensionsElementWithKeyInfo') === 'true',
                    artifactBindingResponse: formData.get('artifactBindingResponse') === 'true',
                },
            };

            if (!idpData.alias) {
                showErrorToast('请输入IDP别名');
                return;
            }

            if (!idpData.config.singleSignOnServiceUrl) {
                showErrorToast('请输入SSO Service URL');
                return;
            }

            try {
                await updateIdpInstance(currentRealm, idpData);
                showSuccessToast('IDP实例更新成功');
                closeAllModals();
                loadIdpModule();
            } catch (error) {
                showErrorToast('更新IDP实例失败: ' + error.message);
            }
        });

        // 折叠面板切换
        const metadataHeader = document.getElementById('metadataHeader');
        const metadataContent = document.getElementById('metadataContent');
        metadataHeader.addEventListener('click', () => {
            metadataHeader.classList.toggle('collapsed');
            metadataContent.classList.toggle('collapsed');
        });

        // 文件选择后更新显示
        document.getElementById('samlFile').addEventListener('change', (e) => {
            const fileLabel = document.getElementById('fileLabel');
            if (e.target.files.length > 0) {
                fileLabel.textContent = e.target.files[0].name;
            }
        });

        // 导入并解析SAML Metadata
        document.getElementById('importBtn').addEventListener('click', async () => {
            const fileInput = document.getElementById('samlFile');
            if (!fileInput.files || fileInput.files.length === 0) {
                showErrorToast('请先选择SAML Metadata文件');
                return;
            }

            const formData = new FormData();
            formData.append('file', fileInput.files[0]);

            try {
                const result = await importSamlMetadata(currentRealm, formData);
                showSuccessToast('SAML Metadata解析成功');
                
                // 回填解析结果到表单
                // 支持两种数据结构：result.config 或直接在result中
                const config = result && result.config ? result.config : result;
                
                if (config) {
                    // 回填基础配置
                    if (config.alias) {
                        document.getElementById('idpAlias').value = config.alias;
                    }
                    if (config.displayName) {
                        document.getElementById('idpDisplayName').value = config.displayName;
                    }
                    
                    // 回填元数据配置
                    if (config.entityId) {
                        document.getElementById('idpEntityId').value = config.entityId;
                    }
                    if (config.singleSignOnServiceUrl) {
                        document.getElementById('idpSsoUrl').value = config.singleSignOnServiceUrl;
                    }
                    if (config.singleLogoutServiceUrl) {
                        document.getElementById('idpSingleLogoutServiceUrl').value = config.singleLogoutServiceUrl;
                    }
                    if (config.validateSignature !== undefined) {
                        document.getElementById('idpValidateSignature').value = config.validateSignature.toString();
                    }
                    if (config.signingCertificate) {
                        document.getElementById('idpSigningCertificate').value = config.signingCertificate;
                    }
                    if (config.postBindingLogout !== undefined) {
                        document.getElementById('idpPostBindingLogout').value = config.postBindingLogout.toString();
                    }
                    if (config.nameIDPolicyFormat) {
                        document.getElementById('idpNameIDPolicyFormat').value = config.nameIDPolicyFormat;
                    }
                    if (config.loginHint) {
                        document.getElementById('idpLoginHint').value = config.loginHint;
                    }
                    if (config.metadataDescriptorUrl) {
                        document.getElementById('idpMetadataDescriptorUrl').value = config.metadataDescriptorUrl;
                    }
                    if (config.enableFormMetadata !== undefined) {
                        document.getElementById('idpEnableFormMetadata').value = config.enableFormMetadata.toString();
                    }
                    if (config.postBindingAuthnRequest !== undefined) {
                        document.getElementById('idpPostBindingAuthnRequest').value = config.postBindingAuthnRequest.toString();
                    }
                    if (config.wantAuthnRequestsSigned !== undefined) {
                        document.getElementById('idpWantAuthnRequestsSigned').value = config.wantAuthnRequestsSigned.toString();
                    }
                    if (config.artifactResolutionServiceUrl) {
                        document.getElementById('idpArtifactResolutionServiceUrl').value = config.artifactResolutionServiceUrl;
                    }
                    if (config.addExtensionsElementWithKeyInfo !== undefined) {
                        document.getElementById('idpAddExtensionsElementWithKeyInfo').value = config.addExtensionsElementWithKeyInfo.toString();
                    }
                    if (config.artifactBindingResponse !== undefined) {
                        document.getElementById('idpArtifactBindingResponse').value = config.artifactBindingResponse.toString();
                    }
                }
            } catch (error) {
                showErrorToast('解析SAML Metadata失败: ' + error.message);
            }
        });

        // Mapper折叠面板切换
        const mapperHeader = document.getElementById('mapperHeader');
        const mapperContent = document.getElementById('mapperContent');
        mapperHeader.addEventListener('click', () => {
            mapperHeader.classList.toggle('collapsed');
            mapperContent.classList.toggle('collapsed');
            // 如果展开，加载mapper列表
            if (!mapperHeader.classList.contains('collapsed')) {
                loadMappersForIdp(alias);
            }
        });

        // 加载IDP的Mapper列表
        async function loadMappersForIdp(idpAlias) {
            const container = document.getElementById('mapperListContainer');
            if (!container) return;

            container.innerHTML = '<div class="loading">加载中</div>';

            try {
                const mappers = await listIdpMappers(currentRealm, idpAlias);
                renderMapperList(mappers, idpAlias);
            } catch (error) {
                container.innerHTML = `<div class="empty-state"><p>加载Mapper列表失败: ${error.message}</p></div>`;
            }
        }

        function renderMapperList(mappers, idpAlias) {
            const container = document.getElementById('mapperListContainer');
            if (!container) return;

            if (!mappers || mappers.length === 0) {
                container.innerHTML = '<div class="empty-state"><p>暂无Mapper</p></div>';
                return;
            }

            container.innerHTML = `
                <table>
                    <thead>
                        <tr>
                            <th>名称</th>
                            <th>属性键</th>
                            <th>属性值</th>
                            <th>友好名称</th>
                            <th>操作</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${mappers.map(mapper => `
                            <tr data-mapper-id="${mapper.id}">
                                <td>${mapper.name || '-'}</td>
                                <td>${mapper.attributeKey || '-'}</td>
                                <td>${mapper.attributeValue || '-'}</td>
                                <td>${mapper.friendlyName || '-'}</td>
                                <td>
                                    <button class="btn btn-link" onclick="deleteMapperHandler('${idpAlias}', '${mapper.id}')">删除</button>
                                </td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            `;
        }

        // 添加Mapper按钮
        document.getElementById('showCreateMapperModalBtn').addEventListener('click', () => {
            showCreateMapperModal(alias, () => {
                loadMappersForIdp(alias);
            });
        });
    } catch (error) {
        showErrorToast('加载IDP信息失败: ' + error.message);
    }
}

function deleteIdpHandler(alias) {
    showConfirmDialog(`确定要删除IDP实例 "${alias}" 吗？`, async () => {
        try {
            await deleteIdpInstance(currentRealm, alias);
            showSuccessToast('IDP实例删除成功');
            loadIdpModule();
        } catch (error) {
            showErrorToast('删除IDP实例失败: ' + error.message);
        }
    });
}

// ==================== 将函数暴露到全局作用域 ====================
window.loadUserModule = loadUserModule;
window.loadGroupModule = loadGroupModule;
window.loadRoleModule = loadRoleModule;
window.loadIdpModule = loadIdpModule;
window.viewUser = viewUser;
window.viewGroup = viewGroup;
window.toggleSelectAllGroups = toggleSelectAllGroups;
window.showEditGroupModal = showEditGroupModal;
window.showCreateGroupModal = showCreateGroupModal;
window.deleteGroupHandler = deleteGroupHandler;
window.showCreateRoleModal = showCreateRoleModal;
window.showEditRoleModal = showEditRoleModal;
window.deleteRoleHandler = deleteRoleHandler;
window.showCreateIdpModal = showCreateIdpModal;
window.showEditIdpModal = showEditIdpModal;
window.deleteIdpHandler = deleteIdpHandler;
window.deleteMapperHandler = deleteMapperHandler;
window.showCreateMapperModal = showCreateMapperModal;
window.addRole = addRole;
window.removeRole = removeRole;
window.renderSelectedRoles = renderSelectedRoles;
window.addUser = addUser;
window.removeUser = removeUser;
window.renderSelectedUsers = renderSelectedUsers;
