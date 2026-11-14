import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import {
  schemaLeaves, evaluateRowGroup, matchesFilter, compareLiteral,
  decodeStatValue, literalFor
} from '../src/parquet/meta.js';
import { planKey } from '../src/collect.js';

const require = createRequire(import.meta.url);
const parquetThrift = require('parquetjs-lite/gen-nodejs/parquet_types.js');

const utf8 = value => Buffer.from(value, 'utf8');
const double = value => { const b = Buffer.alloc(8); b.writeDoubleLE(value); return b; };

const schema = [
  { name: 'root', num_children: 3 },
  { name: 'date', type: parquetThrift.Type.BYTE_ARRAY, converted_type: parquetThrift.ConvertedType.UTF8, repetition_type: 1 },
  { name: 'rate', type: parquetThrift.Type.DOUBLE, repetition_type: 1 },
  { name: 'lanes', repetition_type: 2, num_children: 1 },
  { name: 'name', type: parquetThrift.Type.BYTE_ARRAY, converted_type: parquetThrift.ConvertedType.UTF8, repetition_type: 1 }
];
const leavesByPath = new Map(schemaLeaves(schema).map(leaf => [leaf.path, leaf]));

const rowGroupWith = statistics => ({
  columns: [{ meta_data: { path_in_schema: ['date'], statistics } }]
});
const eq = value => [{ path: 'date', op: 'eq', value }];

test('统计值缺失时保留行组（多取不漏取）', () => {
  assert.equal(evaluateRowGroup(rowGroupWith(null), eq('2026-11-02'), leavesByPath).decision, 'keep');
  assert.equal(evaluateRowGroup(rowGroupWith(null), eq('2026-11-02'), leavesByPath).reasons[0].reason, 'statistics-missing');
  const noMinMax = evaluateRowGroup(rowGroupWith({ null_count: 2 }), eq('2026-11-02'), leavesByPath);
  assert.equal(noMinMax.decision, 'keep');
  assert.equal(noMinMax.reasons[0].reason, 'statistics-without-min-max');
  const noChunk = evaluateRowGroup({ columns: [] }, eq('2026-11-02'), leavesByPath);
  assert.equal(noChunk.decision, 'keep');
  assert.equal(noChunk.reasons[0].reason, 'column-chunk-missing');
});

test('字符串边界：等于边界的值保留，严格越界才跳过', () => {
  const statistics = { min_value: utf8('2026-11-02'), max_value: utf8('2026-11-03') };
  assert.equal(evaluateRowGroup(rowGroupWith(statistics), eq('2026-11-02'), leavesByPath).decision, 'keep');
  assert.equal(evaluateRowGroup(rowGroupWith(statistics), eq('2026-11-03'), leavesByPath).decision, 'keep');
  const above = evaluateRowGroup(rowGroupWith(statistics), eq('2026-11-04'), leavesByPath);
  assert.equal(above.decision, 'skip');
  assert.equal(above.reasons[0].reason, 'above-max');
  const below = evaluateRowGroup(rowGroupWith(statistics), eq('2026-11-01'), leavesByPath);
  assert.equal(below.decision, 'skip');
  assert.equal(below.reasons[0].reason, 'below-min');
});

test('统计值被标记为截断时不按该侧跳过', () => {
  const statistics = {
    min_value: utf8('2026-11-02'), max_value: utf8('2026-11-03'),
    is_max_value_exact: false
  };
  const result = evaluateRowGroup(rowGroupWith(statistics), eq('2026-11-09'), leavesByPath);
  assert.equal(result.decision, 'keep');
  assert.equal(result.reasons[0].reason, 'statistics-not-exact');
});

test('UTF8 统计值按无符号字节序比较，而非 UTF-16 码元序', () => {
  // U+E000 的 UTF-8 首字节 0xEE 小于 '😀' 的 0xF0（字节序在区间之外），
  // 但 UTF-16 码元 0xE000 大于高代理项 0xD83D（码元序会被误判为区间内）
  const statistics = { min_value: utf8('😀'), max_value: utf8('😃') };
  const result = evaluateRowGroup(rowGroupWith(statistics), eq('\uE000'), leavesByPath);
  assert.equal(result.decision, 'skip');
  assert.equal(result.reasons[0].reason, 'below-min');
  assert.ok(compareLiteral(literalFor(eq('\uE000')[0], leavesByPath.get('date')),
    decodeStatValue(utf8('😀'), leavesByPath.get('date'))) < 0);
});

test('数值统计值按数值比较', () => {
  const leaf = leavesByPath.get('rate');
  const statistics = { min_value: double(1.5), max_value: double(2.5) };
  const rowGroup = { columns: [{ meta_data: { path_in_schema: ['rate'], statistics } }] };
  const term = value => [{ path: 'rate', op: 'eq', value }];
  assert.equal(evaluateRowGroup(rowGroup, term('2.5'), leavesByPath).decision, 'keep');
  assert.equal(evaluateRowGroup(rowGroup, term('2.5000001'), leavesByPath).decision, 'skip');
  assert.equal(evaluateRowGroup(rowGroup, term('1.4'), leavesByPath).decision, 'skip');
  assert.ok(leaf);
});

test('记录级筛选：空值不匹配，重复字段任一元素命中即命中', () => {
  const dateEq = [{ path: 'date', op: 'eq', value: '2026-11-02' }];
  assert.ok(matchesFilter({ date: '2026-11-02' }, dateEq, leavesByPath));
  assert.ok(!matchesFilter({ date: '2026-11-01' }, dateEq, leavesByPath));
  assert.ok(!matchesFilter({}, dateEq, leavesByPath)); // 字段缺失（空值）不匹配
  const laneEq = [{ path: 'lanes.name', op: 'eq', value: '西线' }];
  assert.ok(matchesFilter({ lanes: [{ name: '东线' }, { name: '西线' }] }, laneEq, leavesByPath));
  assert.ok(!matchesFilter({ lanes: [{ name: '东线' }] }, laneEq, leavesByPath));
  assert.ok(!matchesFilter({}, laneEq, leavesByPath));
});

test('计划键随对象版本与投影变化，已校验范围不会错误命中', () => {
  const base = { url: 'u', etag: 'a', projection: ['date'], filter: [] };
  assert.notEqual(planKey(base), planKey({ ...base, etag: 'b' }));
  assert.notEqual(planKey(base), planKey({ ...base, projection: ['date', 'rate'] }));
  assert.notEqual(planKey(base), planKey({ ...base, filter: [{ path: 'date', op: 'eq', value: 'x' }] }));
  assert.equal(planKey(base), planKey({ ...base }));
});
