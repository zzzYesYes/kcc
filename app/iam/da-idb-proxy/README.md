# da-idb-proxy
Data Agent身份认证服务

## keycloak部署准备
### 安装Data Agent订制token插件
1. 在custom-token-mapper下打包：
    jar cvf data-agent-mapper.jar .\data-agent-mapper.js .\META-INF\keycloak-scripts.json
2. 将新生成的jar文件放入keycloak安装目录的providers下
3. keycloak 26.5使用 script-<js脚本文件名> 的规则索引订制mapper插件，因此需要提供环境变量：
    KC_SCRIPT_MAPPER=script-data-agent-mapper.js
4. 启动keycloak时附带参数 --features=scripts，示例（windows）：
    .\bin\kc.bat start-dev --features=scripts

## REST 接口
见doc/idb-proxy-api.json和doc/api-description
