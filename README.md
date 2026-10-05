# HTTP 代理验收样本

仅依赖 Python 3.12 标准库。设置 `UPSTREAM_URL=http://上游:端口`，运行 `uv run --project backend python examples/http-proxy/proxy.py`。

监听 8080，将请求的方法、路径和内容转发给指定上游：总预算 350ms，GET/HEAD 最多尝试 3 次，POST 不重试，同时最多 2 个上游请求；客户端断开时取消上游并释放连接。

平台独立验收位于 `backend/app/acceptance/http_proxy.py`，项目内测试只验证基础配置。该样本用于实验，不作为生产代理。
