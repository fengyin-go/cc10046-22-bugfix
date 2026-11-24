"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

链路路由先注册：`/api/chains/...` 的固定核对入口不能被通用的
`/api/{module}/{entry_id}` 动态路径抢先匹配。
"""
from __future__ import annotations

from app.routers import chains
from app.routers import resource

ROUTERS = [chains, resource]
