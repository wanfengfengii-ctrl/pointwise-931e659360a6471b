# 离线浏览器采集端口契约

两代端口为本工程独立实现的离线依赖，均仅使用标准库。SDK1 不通过 SDK2 实现，SDK2 不调用 SDK1。它们不执行网页交互，不安装真实浏览器。业务数据是响应资料，不进行解压、HTTP 缓存或正文提取。

## SDK1

vendor.sdk1.Hop(url,status,headers,extra_headers=None) 表示一个已组合完整响应跳。headers 是有序文本二元组序列；extra_headers 为独立序列或 None。

CompleteChain(session_id,request_id,hops,body,failure=None) 转移完整响应链及不可变 bytes 正文。failure 非空表示网络失败、body 必须 None，hops 是失败前已确认响应。NetworkFailure(session_id,request_id,hops,reason) 是同一网络失败的异常形式。Source(targets,outcomes) 的 outcomes 按 URL 保存有限结果序列，fetch(url) 消耗一个完整结果，NetworkFailure 结果抛出；无剩余结果抛 SourceExhausted。targets 是公开采集批次的 Target(url,request_key) 序列；直接 run 可使用没有 targets 的源。calls 记录实际 fetch 的 URL，便于核账。

SDK1 的 hops 按重定向顺序排列，第一跳为目标 URL。每次 fetch 是独立采集，即便 URL、session_id 和 request_id 与历史相同。完整对象不存在尚待回填的头；业务可以在一次事务中发布。

## SDK2

Response(session_id,request_id,url,status,headers,expects_extra)
ExtraHeaders(session_id,request_id,headers)
BodyChunk(session_id,request_id,data)
Finished(session_id,request_id)
Failed(session_id,request_id,reason)

BodyChunk 仅用于输入 Player 的确定性样本，data 为 bytes。Player.play(callback) 对它调用 BorrowedBody(session_id,request_id,data)，data 为 memoryview；其他事件原样交付。所有回调同步进行，异常原样传播并停止播放。正文缓冲在回调返回或异常时立即复用和覆盖，仅在该回调期间可读；保留数据需要在回调内取得独立字节。

(session_id,request_id) 是请求链键；同链所有重定向复用 request_id，不同 session 的相同 request_id 不同链。Response 在各自链内按跳次排序。ExtraHeaders 不包含 URL、跳次、状态码或响应对象引用，只承诺按本链中 expects_extra=True 的跳次顺序到达；可早于所属 Response，也可晚于后续 Response，所有链可任意交错。不要求附加头字段唯一；不能用附加头覆盖基础头。expects_extra=False 的跳无 ExtraHeaders，True 允许空序列。

正文块按链内正文顺序交付，拼接为最终跳的完整原始正文；存在但为空的正文用一个空块表达，失败链没有正文块。Finished 或 Failed 各链恰有一个，表示网络结束，不表示声明的附加头已交付。网络结束后还可出现 ExtraHeaders。有效成功样本在播放结束时有全部声明的 ExtraHeaders 和完整正文。失败样本保留已经发生的响应及其声明的附加头，Failed.reason 非空。Failed 后不再有 Response 或正文。

Player 是原始事件传输端口，仅校验事件类型、非空键和样本正文类型，不为消费方检查整链语义、不等待齐备、不编号跳次、不持久化。可用它播放不完整前缀来观察边界。`from_records(records)` 将 JSON 事件列表转为 Player，kind 为 response/extra/body/finished/failed；body 用 hex，其他字段同上述事件。错误样本字段抛 PortError。

capture/sample_events.json 的明确事实：tab-a/r 的 302 附加头在 200 响应之后到达，200 附加头在 Finished 后到达；tab-b/r 的附加头早于它的响应；tab-c/no-extra 的正文存在但为空且不需要附加头；tab-d/broken 保留重定向和失败原因，附加头在 Failed 后到达。SDK1 与 SDK2 样本分别编写，并非由某个适配器互相生成。
