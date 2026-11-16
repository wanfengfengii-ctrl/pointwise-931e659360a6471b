# 浏览器响应证据采集库

Python 3.11 标准库工程。公开入口收集浏览器网络响应资料，将任务、尝试、重定向链、正文和采集回执保存在 SQLite，支持失败重试与历史回查。当前使用题内独立离线 SDK1 端口；无需浏览器安装或网络。端口由本工程提供，不声称是外部厂商发行包。

运行完整回归与演示：

```sh
python3.11 -B -m unittest discover -s tests -v
python3.11 -B -m capture.demo
```

演示在测试临时目录创建历史库，关闭重开后逐链输出真实归档结果，退出时清理。测试的磁盘数据也只写测试临时目录。安装步骤为空。

## 正常业务与接口

`Database(path=':memory:', fault=None)` 持有连接，支持上下文管理。磁盘路径由调用方显式传入；默认使用内存库。单工作者运行，不支持同一连接的并发调用或嵌套事务。已有非 v1 库以 `InvalidState` 拒绝打开。

`Jobs(db).enqueue(url, request_key)` 登记合法 HTTP(S) URL 并创建第 1 次待采集尝试，返回整数任务 ID。请求键非空且永久绑定原 URL；相同键与 URL 重放返回原任务，不新增尝试；相同键绑定其他 URL 抛 `Conflict`。URL 不做去重或规范化，同 URL 不同请求键是不同任务。URL 不能带认证信息或空白。

`run(job_id, source, db)` 对当前 pending 尝试调用一次 SDK1 的 `source.fetch(url)`，返回 `{'chain_id': str, 'state': 'succeeded'|'failed'}`。网络等待期间不持写事务。SDK1 完整链中每跳保留 URL、状态码、基础头及独立附加头。两个头序列均保留原名、顺序和重复项，附加头不会覆盖基础头；附加头缺席是 null，存在但为空是 []。成功采集要求最终响应和 bytes 正文；HTTP 404 等仍可成功采集。空正文有真实正文引用，不等于失败的无正文。

`capture(source, db)` 依次采集 SDK1 源的有限 `targets`（Target(url,request_key)），返回上述结果列表。已终态的请求键只回放原结果，不消耗源；中途异常会传播，先前已完成任务仍保留。它不提供跨目标的整批原子性。

`Jobs(db).retry(job_id)` 仅对最近已失败的任务新增 pending 尝试，返回新尝试编号；历史失败链和回执不变。网络失败由完整失败结果或 SDK1 `NetworkFailure` 表达，保存已确认跳次与原因，没有成功正文。任务可在零响应时失败。重复 capture 失败请求不会暗中重试。

`Jobs(db).get_job(job_id)` 返回任务字典及按 number 排序的 attempts，每项包含 number、state、chain_id。任务状态反映最近尝试；未知任务返回 None。

`Archive(db).get_chain(chain_id)` 返回链字典，包含 chain_id、job_id、attempt、session_id、request_id、state、failure、body_ref、body_position 和按 position 排序的 hops。链 ID 是本库的稳定采集身份，SDK 请求编号仅为来源证据；相同 SDK 请求编号或 URL 再次采集不能复用历史链。body_position 是成功链最终跳位置，失败为 None。hops 每项含 position、url、status、headers、extra_headers。未知链返回 None。

`Archive(db).get_receipt(chain_id)` 返回持久采集回执快照，含 receipt_id、chain_id、job_id、attempt、state、final_url、body_ref、body_sha256、failure；未知链返回 None。回执不会因重试、同 URL 再采或查询返回值被调用方修改而改变。

`Bodies(db).read_body(body_ref)` 或 `Archive(db).read_body(body_ref)` 返回原字节，同时核对长度和 SHA-256。缺失或损坏抛 `IntegrityError`，不返回部分字节；来源链与原回执仍可查询。`Bodies(db).metadata(body_ref)` 返回保存的引用、长度与摘要。

最小调用：

```python
from capture import Database, Jobs, Archive, Bodies, capture
from capture.samples import source

with Database() as db:
    result = capture(source('redirect', 'request-42'), db)[0]
    chain = Archive(db).get_chain(result['chain_id'])
    assert Bodies(db).read_body(chain['body_ref']) == b'final\x00bytes'
    assert len(chain['hops']) == 2
```

## 状态、持久化和失败

任务拥有有序尝试，每次尝试至多有一条终态链。成功链有最终正文引用，失败链有明确原因且无正文。中间跳须为 301/302/303/307/308，成功最终跳为其他 200–599 响应。SDK1 的第一跳必须属于登记的原 URL。非法来源链抛 `ProtocolError` 且尝试仍 pending。`InvalidInput` 表示非法登记参数，`NotFound` 表示运行或重试未知任务，`InvalidState` 表示无待采尝试、非法重试或嵌套事务。这些类型均在 `capture.errors`。有限源用尽抛 SDK1 `SourceExhausted`，不能伪装成网络失败。

正文、链、跳次、回执和任务终态在同一个 `Database.atomic()` SQLite 事务提交。正文保存或提交异常传播且全部回滚，待采尝试不变；调用方可以重新获取源并再次 run 同一尝试。只有已归档的网络失败才需要显式 retry 创建新尝试。故障注入对象 `FaultPlan` 的 arm(stage,after=0) 在指定边界一次抛 `InjectedFailure`：body.before、body.after、archive.after、jobs.after、before_commit、migration.after、v2.event.before、v2.event.after、v2.seal.before、v2.seal.after。`after` 是跳过该边界的次数，不是时间。

历史表是 jobs、attempts、chains_v1、hops_v1、bodies_v1、receipts_v1；有序头为 JSON，正文为 BLOB。新建库 meta.storage_version=1，`upgrade(db)` 之后为 2，并另建 v2_streams、v2_staged_chains、v2_staged_responses、v2_staged_extras、v2_staged_bodies 五张事件暂存表。表关系和原子事务在 capture/db.py，任务状态在 jobs.py，完整响应接纳在 acquisition.py，正文完整性在 bodies.py，证据与回执在 archive.py，v2 采集与升级在 capture/v2.py。底层连接通过 db.connection 暴露，供维护和核账；普通业务写入应使用上述入口。

## SDK2 事件采集与升级

`upgrade(db)` 把已有 v1 库幂等地迁到存储版本 2：建五张 v2 表并在同一事务内更新 meta.storage_version。v1 历史行不重写；迁移事务内任意中断（含 migration.after、before_commit）全部回滚，重开后仍是完整 v1，不暴露半迁移库。对新库可直接调用，也可在 `capture_v2` 内隐式完成；对已是 v2 的库重复调用不改动任何行。

`capture_v2(player, db, limit=None)` 采样 SDK2 `Player` 的整段事件序列（Session 重开后可再次提供同一序列），返回列表类 `CaptureV2Result`，每项为本次新封存链的 `{'chain_id','state'}`，并带 stream_id、cursor、event_count、complete。规则如下：

- `(session_id, request_id)` 是链键，不同 session 的同名 request_id 是不同链。Response 按各链到达顺序编号为 position 0..n，首跳即登记 URL。暂存键另以请求键 `v2:<stream>:["<session>","<request>"]` 绑定任务，重采同一链不会新建任务。
- ExtraHeaders 可早于所属 Response，也可晚于后续 Response 甚至 Finished；只按本链 expects_extra=True 的跳次顺序归属，绝不覆盖基础头。expects_extra=False 的跳为 null，True 允许 []。
- BodyChunk 在回调内立即复制为独立 bytes 并按链拼接为最终跳正文；存在但为空的正文字节为 b''，失败链没有正文块。端口缓冲复用不会泄漏到归档。
- Finished/Failed 只表示网络结束，不代表附加头齐备。链在“网络已结束 + 所有声明附加头已到 + 成功链至少一个正文块”同时满足时才封存；已确认跳次在失败链中保留，Failed.reason 非空。
- 每个事件边界都可用 `FaultPlan` 注入中断。每个事件应用、可封存链发布和 stream cursor 前进在同一个事务；重开再播放同一序列时按持久 cursor 跳过已应用事件，已封存链不重发、不重复建任务。`limit` 限制本次最多消费多少个尚未应用的事件（None 消费全部，0 为仅登记），用于分段续采。结构非法事件抛 `ProtocolError` 并只回滚该事件，先前已提交事件和链保持。
- 新链与 v1 历史共用 jobs/attempts/chains_v1/hops_v1/bodies_v1/receipts_v1，因此 `Archive.get_chain`、`get_receipt`、`Bodies.read_body/metadata`、`Jobs.get_job/retry` 对两者一致可查；v2 失败链同样可经 `Jobs.retry` 走 SDK1 重采。新增链沿用本库 `c<job>.<attempt>` 链 ID 与 `b<job>.<attempt>` 正文引用，跨链身份不冲突。
- `stream_state(db, stream_id)` 返回持久 cursor 视图，便于核对续采位置。

## 有限样例与离线端口

capture/sample_data.json 提供单跳、空正文、重复头重定向和失败四组明确数据；`capture.samples.source(name, request_key=None, raise_network=False)` 创建 SDK1 有限源。SDK1 Source 每次 fetch 消耗一个预置结果；使用真实端口队列，不模拟数据库或事务。

`capture.samples.build_history(path)` 在空库中经公开业务入口生成可重开的真实 v1 历史，包含六条链、五个任务、一次失败及重试、同 URL 独立采集和空正文。返回 chains、retry_job、duplicate_job。非空库拒绝；不覆盖既有历史。

vendor/sdk2.py 提供独立原始事件端口及 Player、from_records。事件字段、顺序、回调借用缓冲区和终态语义见 SDK2 类文档及 PORTS.md。capture/sample_events.json 是固定的跨请求、跨 session、迟到附加头、空正文与失败事件样本。tests/test_ports.py 直接核对端口交付顺序及缓冲有效期，不经过业务归档。

`python3.11 -B -m capture.demo` 演示 SDK1 历史；`python3.11 -B -m capture.demo_v2` 在临时目录生成 v1 历史、升级、分段续采 SDK2 样本，并逐项打印期望值与旧接口实际值的对照，含跨链交错、迟到附加头、正文缓冲复用、协议失败与中断续采。

## 回归覆盖

tests/test_collection.py 覆盖成功、空正文、重定向有序重复头、回执、请求重放及坏来源拒绝；test_jobs.py 覆盖登记、键冲突和重试资格；test_retry.py 覆盖失败证据、显式重试和零跳失败；test_failures.py 覆盖所有提供的存储边界回滚；test_recall.py 覆盖同 URL 不同版本、查询副本及正文损坏；test_history.py 在测试临时目录核对重开后的完整历史和提交失败重跑；test_ports.py 独立检验两代端口；test_v2.py 独立覆盖 v2 升级幂等与中断、跨链交错、迟到附加头归属、正文缓冲复用复制、网络结束后等待附加头、失败链证据、limit 分段续采与重放不重发、v2 失败重试及 v1/v2 混合历史。

范围为单工作者离线响应资料采集，不包含网页操作、登录和正文抽取或分布式调度。
