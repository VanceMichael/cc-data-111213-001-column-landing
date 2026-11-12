import { mkdir as ensureFixtureDirectory } from 'node:fs/promises';
await ensureFixtureDirectory('fixtures', {recursive:true});

import parquet from 'parquetjs-lite';
const schema = new parquet.ParquetSchema({ date: { type: 'UTF8' },
  supplier: { type: 'UTF8' }, rate: { type: 'DOUBLE', optional: true },
  lanes: { repeated: true, fields: { name: { type: 'UTF8' }, enabled: { type: 'BOOLEAN' } } } });
const writer = await parquet.ParquetWriter.openFile(schema, 'fixtures/rates.parquet', { rowGroupSize: 2 });
for (const row of [
  { date: '2026-11-01', supplier: '甲', rate: 1.2, lanes: [{ name: '东线', enabled: true }] },
  { date: '2026-11-01', supplier: '乙', lanes: [] },
  { date: '2026-11-02', supplier: '甲', rate: 0, lanes: [{ name: '西线', enabled: false }] },
  { date: '2026-11-03', supplier: '丙', rate: 4.1, lanes: [] }
]) await writer.appendRow(row);
await writer.close();
