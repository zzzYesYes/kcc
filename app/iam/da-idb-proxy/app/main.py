import os
from dotenv import load_dotenv

# 必须在导入应用模块之前加载环境变量
load_dotenv()
load_dotenv('.env.local', override=True)

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from app.core.keycloak import KeycloakError
from app.api.v1 import tenants, idp, identity, common, token

app = FastAPI(title="Keycloak Business Wrapper", version="1.0.0")

@app.exception_handler(KeycloakError)
async def global_kc_exception_handler(request: Request, exc: KeycloakError):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

# 挂载路由，注意顺序
app.include_router(tenants.router, prefix="/api/v1")
app.include_router(idp.router, prefix="/api/v1")
app.include_router(identity.router, prefix="/api/v1")
app.include_router(common.router, prefix="/api/v1")
app.include_router(token.router, prefix="/api/v1")
'''[仅供演示!!!]挂载静态文件服务 开始'''
ui_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui")
app.mount("/ui", StaticFiles(directory=ui_path), name="ui")

@app.get("/")
async def read_index():
    # 直接返回该 HTML 文件
    return RedirectResponse(url="/ui/index.html")
'''[仅供演示!!!]挂载静态文件服务 结束'''


@app.get("/api/v1/export-spec", include_in_schema=False)
def export_spec():
    """补全：导出 OpenAPI 规范"""
    return app.openapi()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8090, reload=True)
