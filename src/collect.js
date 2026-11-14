import { createReadStream } from 'node:fs';
import { mkdir, open, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import parquet from 'parquetjs-lite';
import { Archive, digest, writeAtomicFile } from './archive.js';
import { openRemote, fetchRange, runPool, VersionChangedError } from './rangeClient.js';
import {
  MAGIC, TRAILER_SIZE, readTrailer, decodeMetadata, encodeFooter,
  schemaLeaves, pruneSchema, chunkRange, shiftColumnChunk, buildRowGroup,
  buildFileMetadata, evaluateRowGroup, matchesFilter, toSafeNumber
} from './parquet/meta.js';

// 计划键：同一来源版本 + 同一投影 + 同一筛选才复用已校验范围，
// 更换投影或对象版本会得到不同的键，不可能错误命中旧内容。
export function planKey({ url, etag, projection, filter }) {
  return createHash('sha256')
    .update(JSON.stringify({ url, etag, projection, filter }))
    .digest('hex')
    .slice(0, 24);
}

async function savePlan(plan) {
  const { dir, saveQueue, ...document } = plan;
  await writeAtomicFile(join(dir, 'plan.json'), JSON.stringify(document, null, 2));
}

// 多个抓取 worker 会并发标记完成：保存动作排队执行，
// 序列化发生在执行时刻，保证落盘快照不丢已完成的范围
function scheduleSave(plan) {
  plan.saveQueue = (plan.saveQueue || Promise.resolve()).then(() => savePlan(plan));
  return plan.saveQueue;
}

async function fileDigest(path) {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path, { highWaterMark: 1 << 20 })) hash.update(chunk);
  return hash.digest('hex');
}

async function readFileIfExists(path) {
  try { return await readFile(path); } catch { return null; }
}

// 开启一轮领取：获取页脚、评估行组、生成（或恢复）计划并执行。
// 返回 { receipt, planDir, outputPath, recordsPath, records }。
export async function collect({
  url, archiveDir, workdir,
  projection = null,           // 点分列路径；null 表示全部叶子列
  filter = [],                 // [{ path, op: 'eq', value: '字符串字面量' }]
  concurrency = 4,             // 同时范围请求数上限
  retries = 3,                 // 单个范围的瞬时故障重试次数
  maxRestarts = 3              // 领取期间对象换版的整体重开次数上限
} = {}) {
  if (!url) throw new Error('需要对象地址');
  if (!archiveDir) throw new Error('需要归档目录');
  const root = resolve(workdir || '.local/collect');
  await mkdir(root, { recursive: true });

  let restarts = 0;
  for (;;) {
    const remote = await openRemote(url);
    const key = planKey({ url, etag: remote.etag, projection, filter });
    const dir = join(root, key);
    let plan = await loadPlan(dir, { url, remote, projection, filter });
    if (!plan) plan = await createPlan(remote, { url, projection, filter, dir, key, retries });
    try {
      await executePlan(plan, remote, { concurrency, retries });
    } catch (error) {
      if (error instanceof VersionChangedError) {
        restarts += 1;
        if (restarts > maxRestarts) {
          throw new Error(`对象在领取期间被替换超过 ${maxRestarts} 次，放弃本次领取`);
        }
        continue; // 换版后整体重开：新 etag 产生新计划，旧版本范围一律不再使用
      }
      error.message += '；可重新执行同一命令从断点继续领取';
      throw error;
    }
    return await finalize(plan, remote, { archiveDir, restarts, concurrency, retries });
  }
}

// 从远端页脚构建领取计划：解析元数据、评估行组、合并连续范围
async function createPlan(remote, { url, projection, filter, dir, key, retries }) {
  const trailer = await fetchRange(remote, remote.size - TRAILER_SIZE, remote.size - 1, { retries });
  const { metadataSize } = readTrailer(trailer);
  const metadataStart = remote.size - TRAILER_SIZE - metadataSize;
  if (metadataStart < MAGIC.length) throw new Error('页脚元数据大小不合法');
  const metadataBytes = await fetchRange(remote, metadataStart, remote.size - TRAILER_SIZE - 1, { retries });
  const metadata = decodeMetadata(metadataBytes);

  const leaves = schemaLeaves(metadata.schema);
  const leavesByPath = new Map(leaves.map(leaf => [leaf.path, leaf]));
  const effectiveProjection = projection && projection.length ? projection : leaves.map(leaf => leaf.path);
  for (const path of effectiveProjection) {
    if (!leavesByPath.has(path)) {
      throw new Error(`投影列不存在: ${path}（可选: ${leaves.map(leaf => leaf.path).join(', ')}）`);
    }
  }
  for (const term of filter) {
    if (!leavesByPath.has(term.path)) throw new Error(`筛选字段不存在: ${term.path}`);
    if (term.op !== 'eq') throw new Error(`暂不支持的筛选方式: ${term.op}`);
  }
  const projectionSet = new Set(effectiveProjection);

  const decisions = metadata.row_groups.map((rowGroup, index) => ({
    index,
    numRows: toSafeNumber(rowGroup.num_rows, '行组行数'),
    ...evaluateRowGroup(rowGroup, filter, leavesByPath)
  }));

  // 需要的列块（保留行组 × 投影列），按对象内偏移排序后合并连续范围
  const chunks = [];
  metadata.row_groups.forEach((rowGroup, index) => {
    if (decisions[index].decision !== 'keep') return;
    for (const column of rowGroup.columns) {
      if (column.file_path) throw new Error('不支持引用外部文件的列块');
      const path = column.meta_data.path_in_schema.join('.');
      if (!projectionSet.has(path)) continue;
      const { start, end } = chunkRange(column);
      chunks.push({ rowGroup: index, column: path, start, end: end - 1, bytes: end - start });
    }
  });
  chunks.sort((a, b) => a.start - b.start);
  const ranges = [];
  for (const chunk of chunks) {
    const last = ranges[ranges.length - 1];
    if (last && chunk.start === last.end + 1) {
      last.end = chunk.end;
      last.chunks.push(chunk);
    } else {
      ranges.push({ id: ranges.length, start: chunk.start, end: chunk.end, chunks: [chunk] });
    }
  }
  for (const range of ranges) {
    range.bytes = range.end - range.start + 1;
    range.status = 'pending';
    range.sha256 = null;
  }

  const plan = {
    key, url, etag: remote.etag, size: remote.size,
    projection: effectiveProjection, filter,
    createdAt: new Date().toISOString(),
    footer: {
      trailerStart: remote.size - TRAILER_SIZE,
      metadataStart, metadataSize,
      sha256: digest(metadataBytes)
    },
    decisions, ranges,
    dir
  };
  await mkdir(join(dir, 'ranges'), { recursive: true });
  await writeAtomicFile(join(dir, 'footer.bin'), metadataBytes);
  await savePlan(plan);
  return plan;
}

// 恢复既有计划：逐项校验版本、投影、筛选与已落盘范围的摘要
async function loadPlan(dir, { url, remote, projection, filter }) {
  const raw = await readFileIfExists(join(dir, 'plan.json'));
  if (!raw) return null;
  const plan = JSON.parse(raw);
  const effectiveProjection = projection && projection.length ? projection : plan.projection;
  const consistent = plan.url === url && plan.etag === remote.etag
    && JSON.stringify(plan.projection) === JSON.stringify(effectiveProjection)
    && JSON.stringify(plan.filter) === JSON.stringify(filter);
  if (!consistent) throw new Error(`领取计划 ${dir} 与本次请求不符，请清理后重试`);
  const footerBytes = await readFile(join(dir, 'footer.bin'));
  if (digest(footerBytes) !== plan.footer.sha256) throw new Error(`领取计划 ${dir} 的页脚校验失败`);
  let reused = 0;
  for (const range of plan.ranges) {
    if (range.status !== 'done') continue;
    const spool = join(dir, 'ranges', `${range.id}.bin`);
    const bytes = await readFileIfExists(spool);
    if (bytes && bytes.length === range.bytes && digest(bytes) === range.sha256) {
      range.reused = true;
      reused += 1;
    } else {
      range.status = 'pending';
      range.sha256 = null;
    }
  }
  plan.dir = dir;
  plan.reusedRanges = reused;
  return plan;
}

// 执行计划：有界并发抓取待办范围，逐块校验摘要并落盘，计划随完成进度原子更新
async function executePlan(plan, remote, { concurrency, retries }) {
  const pending = plan.ranges.filter(range => range.status !== 'done');
  await runPool(pending, concurrency, async range => {
    const bytes = await fetchRange(remote, range.start, range.end, { retries });
    const sha256 = digest(bytes);
    const temporary = join(plan.dir, 'ranges', `${range.id}.${randomUUID()}.tmp`);
    await writeFile(temporary, bytes);
    await rename(temporary, join(plan.dir, 'ranges', `${range.id}.bin`));
    range.status = 'done';
    range.sha256 = sha256;
    range.reused = false;
    await scheduleSave(plan);
  });
}

// 装配输出文件：按偏移序流式拼接已校验范围，重写页脚（裁剪 schema、平移列块偏移）
async function assemble(plan) {
  const metadata = decodeMetadata(await readFile(join(plan.dir, 'footer.bin')));
  const projectionSet = new Set(plan.projection);
  const outputPath = join(plan.dir, 'output.parquet');
  const temporary = `${outputPath}.${randomUUID()}.tmp`;
  const handle = await open(temporary, 'wx', 0o600);
  const hash = createHash('sha256');
  let offset = 0;
  let numRows = 0;
  const write = async buffer => {
    hash.update(buffer);
    offset += buffer.length;
    await handle.write(buffer);
  };
  try {
    await write(Buffer.from(MAGIC));
    const chunkOffsets = new Map();
    for (const range of plan.ranges) {
      const rangeStart = offset;
      for await (const chunk of createReadStream(join(plan.dir, 'ranges', `${range.id}.bin`))) {
        await write(chunk);
      }
      for (const item of range.chunks) {
        chunkOffsets.set(`${item.rowGroup}|${item.column}`, rangeStart + (item.start - range.start));
      }
    }
    const schemaElements = pruneSchema(metadata.schema, projectionSet);
    const rowGroups = [];
    for (const decision of plan.decisions) {
      if (decision.decision !== 'keep') continue;
      const source = metadata.row_groups[decision.index];
      numRows += toSafeNumber(source.num_rows, '行组行数');
      const columns = [];
      let firstChunkOffset;
      for (const column of source.columns) {
        const path = column.meta_data.path_in_schema.join('.');
        if (!projectionSet.has(path)) continue;
        const newStart = chunkOffsets.get(`${decision.index}|${path}`);
        if (newStart === undefined) throw new Error(`缺少列块 ${decision.index}|${path} 的已校验范围`);
        if (firstChunkOffset === undefined) firstChunkOffset = newStart;
        columns.push(shiftColumnChunk(column, newStart - chunkRange(column).start));
      }
      rowGroups.push(buildRowGroup(source, columns, firstChunkOffset));
    }
    const footer = encodeFooter(buildFileMetadata({ source: metadata, schemaElements, rowGroups, numRows }));
    await write(footer);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, outputPath);
  return { path: outputPath, sha256: hash.digest('hex'), size: offset, numRows };
}

// 从装配结果中解码结构化记录并做记录级筛选，逐行写出 JSONL
async function extractRecords(plan, outputPath) {
  const metadata = decodeMetadata(await readFile(join(plan.dir, 'footer.bin')));
  const leavesByPath = new Map(schemaLeaves(metadata.schema).map(leaf => [leaf.path, leaf]));
  const recordsPath = join(plan.dir, 'records.jsonl');
  const temporary = `${recordsPath}.${randomUUID()}.tmp`;
  const handle = await open(temporary, 'wx', 0o600);
  const reader = await parquet.ParquetReader.openFile(outputPath);
  let rowsRead = 0;
  let rows = 0;
  try {
    const cursor = reader.getCursor();
    for (;;) {
      const record = await cursor.next();
      if (!record) break;
      rowsRead += 1;
      if (!matchesFilter(record, plan.filter, leavesByPath)) continue;
      rows += 1;
      await handle.write(JSON.stringify(record, (key, value) =>
        (typeof value === 'bigint' ? value.toString() : value)) + '\n');
    }
  } finally {
    await reader.close();
    await handle.close();
  }
  await rename(temporary, recordsPath);
  return { path: recordsPath, rows, rowsRead };
}

// 归档产物并生成凭据：说明少取了哪些行组、实际读取了多少字节
async function finalize(plan, remote, { archiveDir, restarts, concurrency, retries }) {
  const output = await assemble(plan);
  const records = await extractRecords(plan, output.path);
  const archive = new Archive(archiveDir);
  const parquetObject = await archive.storeFile(output.path);
  const recordsObject = await archive.storeFile(records.path);

  const footerBytes = TRAILER_SIZE + plan.footer.metadataSize;
  const chunkBytes = plan.ranges.reduce((sum, range) => sum + range.bytes, 0);
  const credential = {
    kind: 'selective-capture',
    source: { url: plan.url, etag: plan.etag, size: plan.size },
    projection: plan.projection,
    filter: plan.filter,
    plan: { key: plan.key, restarts },
    rowGroups: plan.decisions.map(({ index, numRows, decision, reasons }) => (
      { index, numRows, decision, reasons })),
    summary: {
      rowGroups: plan.decisions.length,
      kept: plan.decisions.filter(d => d.decision === 'keep').length,
      skipped: plan.decisions.filter(d => d.decision === 'skip').length
    },
    transfer: {
      objectBytes: plan.size,
      footerBytes,
      chunkBytes,
      totalBytes: footerBytes + chunkBytes,
      savedBytes: plan.size - footerBytes - chunkBytes,
      ranges: plan.ranges.map(range => ({
        start: range.start,
        end: range.end,
        bytes: range.bytes,
        sha256: range.sha256,
        status: range.reused ? 'reused' : 'fetched',
        chunks: range.chunks.map(({ rowGroup, column, bytes }) => ({ rowGroup, column, bytes }))
      }))
    },
    output: {
      parquet: { sha256: parquetObject.sha256, size: parquetObject.size, rows: output.numRows },
      records: { sha256: recordsObject.sha256, size: recordsObject.size, rows: records.rows, rowsRead: records.rowsRead }
    },
    limits: { concurrency, retries }
  };
  const receipt = await archive.record({
    sha256: parquetObject.sha256,
    size: parquetObject.size,
    metadata: credential
  });
  return { receipt, planDir: plan.dir, outputPath: output.path, recordsPath: records.path, records };
}
