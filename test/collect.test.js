import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Archive, digest } from '../src/archive.js';
import { collect } from '../src/collect.js';
import { inspect, readRecords } from '../src/domain.js';
import { serveObject } from '../src/source.js';
import { openRemote, fetchRange } from '../src/rangeClient.js';

const V1 = 'fixtures/rates-daily.parquet';
const V2 = 'fixtures/rates-daily-v2.parquet';
const PROJECTION = ['date', 'supplier', 'rate', 'lanes.name'];
const FILTER = [{ path: 'date', op: 'eq', value: '2026-11-02' }];

async function workspace() {
  const root = await mkdtemp(join(tmpdir(), 'collect-'));
  return { root, archive: join(root, 'archive'), work: join(root, 'work') };
}

const readJsonl = async path =>
  (await readFile(path, 'utf8')).split('\n').filter(Boolean).map(JSON.parse);

test('按需领取：投影与行组筛选结果与整文件基准一致，且少读字节', async () => {
  const { root, archive, work } = await workspace();
  const source = await serveObject(V1);
  try {
    const { receipt, recordsPath } = await collect({
      url: `${source.url}/object`, archiveDir: archive, workdir: work,
      projection: PROJECTION, filter: FILTER, concurrency: 2
    });
    const credential = receipt.metadata;

    // 行组取舍与原因可解释
    assert.deepEqual(credential.summary, { rowGroups: 4, kept: 2, skipped: 2 });
    const reasons = credential.rowGroups.map(g => [g.decision, g.reasons[0].reason]);
    assert.deepEqual(reasons, [
      ['skip', 'above-max'],
      ['keep', 'statistics-missing'],   // 第 2 个行组没有统计值，必须保留
      ['keep', 'within-min-max'],
      ['skip', 'below-min']
    ]);

    // 实际读取字节少于整文件，且与服务端实际传输一致
    assert.ok(credential.transfer.totalBytes < credential.transfer.objectBytes);
    assert.equal(credential.transfer.totalBytes, source.stats.transmitted);
    assert.equal(credential.transfer.savedBytes,
      credential.transfer.objectBytes - credential.transfer.totalBytes);
    for (const range of credential.transfer.ranges) assert.match(range.sha256, /^[a-f0-9]{64}$/);

    // 每个范围都属于凭据声明的同一对象版本
    const original = await readFile(V1);
    for (const range of credential.transfer.ranges) {
      assert.equal(range.sha256, digest(original.subarray(range.start, range.end + 1)));
    }

    // 结构化记录与整文件读取后过滤投影完全一致（空值、字典编码、嵌套层级）
    const records = await readJsonl(recordsPath);
    const reference = await readRecords(V1, FILTER, PROJECTION);
    assert.deepEqual(records.map(r => projectForCompare(r)), reference);
    assert.equal(records.length, 3);
    assert.ok(!('rate' in records[0]));                    // 空值保持为空
    assert.equal(records[0].supplier, '甲');               // 字典编码列正确解码
    assert.deepEqual(records[2].lanes, [{ name: '东线' }, { name: '西线' }]); // 重复层级未平铺

    // 凭据与产物已归档且可核验
    const store = new Archive(archive);
    const parquet = await store.read(receipt.sha256);
    assert.ok(parquet.length > 0);
    const recordsObject = await store.read(credential.output.records.sha256);
    assert.deepEqual(JSON.parse(`[${recordsObject.toString().trim().split('\n').join(',')}]`).length, 3);
  } finally {
    await source.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('领取中途换版：整体重开，页脚列块字典页同属一个版本', async () => {
  const { root, archive, work } = await workspace();
  const updated = await readFile(V2);
  let swapped = false;
  const source = await serveObject(V1, {
    onRequest: ({ index }) => {
      if (index === 4 && !swapped) { swapped = true; source.replace(updated); }
    }
  });
  try {
    const { receipt, recordsPath } = await collect({
      url: `${source.url}/object`, archiveDir: archive, workdir: work,
      projection: PROJECTION, filter: FILTER, concurrency: 2
    });
    const credential = receipt.metadata;
    assert.equal(credential.plan.restarts, 1);
    assert.equal(credential.source.etag, `"${digest(updated)}"`);

    // 全部范围都属于换版后的对象，没有混入旧版本内容
    for (const range of credential.transfer.ranges) {
      assert.equal(range.sha256, digest(updated.subarray(range.start, range.end + 1)));
    }
    // 结果与新版本整文件基准一致（新版本报价 3.8，旧版本为 3.5）
    const records = await readJsonl(recordsPath);
    const reference = await readRecords(V2, FILTER, PROJECTION);
    assert.deepEqual(records.map(r => projectForCompare(r)), reference);
    assert.equal(records[2].rate, 3.8);
    // 两个版本各有一个独立的领取计划
    assert.equal((await readdir(work)).length, 2);
  } finally {
    await source.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('故障后可从断点继续领取，已校验范围不再重复请求', async () => {
  const { root, archive, work } = await workspace();
  const source = await serveObject(V1);
  try {
    source.failFrom(6); // 页脚与首批范围成功后开始故障
    await assert.rejects(collect({
      url: `${source.url}/object`, archiveDir: archive, workdir: work,
      projection: PROJECTION, filter: FILTER, concurrency: 2
    }), /断点继续/);
    const servedBeforeRecover = source.stats.ranges.map(r => `${r.start}-${r.end}`);

    source.recover();
    const { receipt } = await collect({
      url: `${source.url}/object`, archiveDir: archive, workdir: work,
      projection: PROJECTION, filter: FILTER, concurrency: 2
    });
    const statuses = receipt.metadata.transfer.ranges.map(r => r.status);
    assert.deepEqual(statuses, ['reused', 'reused', 'fetched', 'fetched']);
    // 已校验的两个范围没有再次出现在请求里
    const servedAfterRecover = source.stats.ranges.slice(servedBeforeRecover.length)
      .map(r => `${r.start}-${r.end}`);
    for (const reused of servedBeforeRecover.slice(0, 2)) {
      assert.ok(!servedAfterRecover.includes(reused));
    }
    assert.equal(receipt.metadata.output.records.rows, 3);
  } finally {
    await source.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('更换投影不会命中既有计划的已校验范围', async () => {
  const { root, archive, work } = await workspace();
  const source = await serveObject(V1);
  try {
    await collect({
      url: `${source.url}/object`, archiveDir: archive, workdir: work,
      projection: ['date', 'supplier'], filter: FILTER, concurrency: 2
    });
    const { receipt } = await collect({
      url: `${source.url}/object`, archiveDir: archive, workdir: work,
      projection: ['date', 'rate'], filter: FILTER, concurrency: 2
    });
    assert.equal((await readdir(work)).length, 2); // 不同投影 → 不同计划
    assert.ok(receipt.metadata.transfer.ranges.every(r => r.status === 'fetched'));
  } finally {
    await source.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('全部行组被统计值排除时产出合法的空结果', async () => {
  const { root, archive, work } = await workspace();
  const source = await serveObject(V1);
  try {
    const { receipt, outputPath } = await collect({
      url: `${source.url}/object`, archiveDir: archive, workdir: work,
      projection: PROJECTION, filter: [{ path: 'supplier', op: 'eq', value: '丁' }], concurrency: 2
    });
    assert.equal(receipt.metadata.summary.skipped, 4);
    assert.equal(receipt.metadata.transfer.chunkBytes, 0);
    assert.equal(receipt.metadata.output.records.rows, 0);
    assert.equal((await inspect(outputPath)).rows, 0); // 空 Parquet 仍可被正常打开
  } finally {
    await source.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('整文件模式保留：经同一范围服务取全量并归档核验', async () => {
  const { root, archive } = await workspace();
  const source = await serveObject(V1);
  try {
    const remote = await openRemote(`${source.url}/object`);
    const whole = await fetchRange(remote, 0, remote.size - 1);
    assert.deepEqual(whole, await readFile(V1));
    const store = new Archive(archive);
    const receipt = await store.accept(whole, { source: remote.url });
    assert.equal((await store.read(receipt.sha256)).length, whole.length);
  } finally {
    await source.close();
    await rm(root, { recursive: true, force: true });
  }
});

// 与 domain.projectRecord 相同的投影形状，用于对照（JSONL 记录键路径已展开）
function projectForCompare(record) {
  const projected = {};
  for (const path of PROJECTION) {
    const segments = path.split('.');
    const extract = (value, depth) => {
      if (value === null || value === undefined) return undefined;
      if (depth === segments.length) return value;
      if (Array.isArray(value)) {
        const items = value.map(item => extract(item, depth)).filter(item => item !== undefined);
        return items.length ? items : undefined;
      }
      return extract(value[segments[depth]], depth + 1);
    };
    const value = extract(record, 0);
    if (value !== undefined) projected[path] = value;
  }
  return projected;
}
