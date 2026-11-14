# 列式交付文件的按需范围资料

column-landing 用于合作资料的本地查看和来源保存。整文件模式保留：人工触发整份资料查看与字节级归档，资料对象按内容摘要保存，每次领取保留单独回执。针对大型 Parquet 每日明细，另提供按需领取：只读取指定列（列投影）与可能命中筛选条件的行组（行组筛选），不为几个字段拉取整份对象占满共享出口。

## 运行环境

使用 Node.js 22。依赖已锁定；首次准备执行 `npm ci --ignore-scripts`，正式运行无需下载资料或软件。全部样例使用合成数据与本机来源，不连接生产系统。

```bash
npm run check
npm test
npm run demo
```

`npm run demo` 在临时目录完成两类领取的对照：整文件模式归档样例资料；按需领取经本地范围服务读取 `fixtures/rates-daily.parquet`（含空值、字典编码列与缺统计值的行组），并在领取中途替换对象版本以验证版本一致性，最后把按需结果与整文件基准逐条比对。演示在九十秒内结束，退出时清理临时内容。

## 资料操作

```bash
npm start -- inspect fixtures/rates.parquet
npm start -- capture fixtures/rates.parquet .local/archive   # 整文件模式：保存来源字节
npm start -- collect <对象地址> .local/archive \
  --select date,supplier,lanes.name --where date=2026-11-02 \
  [--workdir .local/collect] [--concurrency 4]
npm start -- list .local/archive
```

`capture` 只表示来源字节已保存，不声称领域流程已自动完成。`collect` 面向支持 HTTP Range 与对象版本校验（etag/If-Match）的远端：先取页脚解析元数据，按统计值评估每个行组是否可能命中 `--where 列=值`（可重复，取交集），再只抓取保留行组中 `--select` 列的字节范围（含字典页），在本地装配成合法 Parquet 并解出结构化记录（JSONL）。原始对象不被修改，也不会整文件下载后改名充当按需采集。

### 版本一致性

同一轮领取以开启时观察到的 etag 为准，页脚、列块、字典页的每个范围请求都携带 `If-Match`；领取期间对象被替换会收到 412（或响应版本不符），本轮整体重开：新版本生成新计划，旧版本的范围一律不再使用，绝不混版拼接。重开次数有上限（默认 3）。

### 行组筛选的取舍原则

统计值缺失、统计值被标记为截断、列块缺失或类型不支持安全排序时一律保留行组（多取不漏取）；仅当统计值严格证明条件值在 [min, max] 之外才跳过。UTF8 统计值按无符号字节序比较。行组内不命中条件的行在记录级筛选中去除，空值不匹配等值条件。

### 可恢复的领取计划

每次领取在工作目录（默认 `.local/collect`）下落盘一份计划：页脚、待取范围、逐范围摘要与完成状态。内存占用与同时请求数有界（`--concurrency`，默认 4；范围逐块落盘，装配与记录解码均为流式）。进程失败后重新执行同一命令即可从断点继续；已校验摘要的范围直接复用。计划键由对象地址、对象版本、投影与筛选共同决定，更换投影或对象版本不会错误命中旧内容。

### 凭据

`collect` 的回执（`npm start -- list` 可见）说明：每个行组的取舍与原因、实际读取的字节（页脚 + 各范围，含摘要与是否复用）、相比整文件节省的字节、换版重开次数，以及产物（裁剪后的 Parquet 与记录 JSONL）在归档中的内容摘要。财务可据此解释少取了哪些行组、实际读了多少字节。

## 目录约定

- `src/domain.js`：现有领域读取和查看接口，记录投影工具。
- `src/archive.js`：内容对象、来源回执与完整性检查。
- `src/collect.js`：按需领取编排（计划、恢复、装配、凭据）。
- `src/rangeClient.js`：带版本钉住的 HTTP 范围客户端与有界并发。
- `src/parquet/meta.js`：页脚编解码、schema 裁剪、统计值评估、行组决策。
- `src/source.js`：本地范围服务（测试 doubles，支持故障注入与中途换版）。
- `fixtures/`：本地资料与领域配置；`rates-daily*.parquet` 含字典编码与缺统计值行组。
- `test/`：已有行为的运行验证，使用独立临时目录。

`scripts/create-fixtures.js` 用于维护者重建合成资料；运行与测试直接读取已提交的资料。

## Linux 环境

`Dockerfile` 使用 Node.js 22 镜像；浏览器项目使用包含 Chromium 的运行镜像。可执行 `docker build -t column-landing:local .` 后运行 `docker run --rm --network none column-landing:local npm test`。测试中的 HTTP、MQTT 和门户都仅绑定回环地址；不需要第二个容器。构建阶段准备依赖，运行时不安装依赖。
