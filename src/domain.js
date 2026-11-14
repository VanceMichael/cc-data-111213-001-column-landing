import parquet from 'parquetjs-lite';
import { schemaLeaves, matchesFilter } from './parquet/meta.js';

export const sample = 'fixtures/rates.parquet';

export async function inspect(filename) {
  const reader = await parquet.ParquetReader.openFile(filename);
  try {
    const cursor = reader.getCursor();
    const preview = [];
    for (let count = 0; count < 3; count += 1) {
      const record = await cursor.next();
      if (!record) break;
      preview.push(record);
    }
    return { rows: Number(reader.getRowCount()),
      fields: reader.schema.fieldList.map(field => ({ path: field.path, optional: field.isOptional })), preview };
  } finally { await reader.close(); }
}

// 读取本地 Parquet 文件的结构化记录，可按领域条件过滤、按点分路径投影
export async function readRecords(filename, filter = [], projection = null) {
  const reader = await parquet.ParquetReader.openFile(filename);
  try {
    const leavesByPath = new Map(schemaLeaves(reader.metadata.schema).map(leaf => [leaf.path, leaf]));
    const cursor = reader.getCursor();
    const records = [];
    for (;;) {
      const record = await cursor.next();
      if (!record) break;
      if (matchesFilter(record, filter, leavesByPath)) records.push(record);
    }
    return projection ? records.map(record => projectRecord(record, projection)) : records;
  } finally { await reader.close(); }
}

// 把记录按点分路径投影为 { 路径: 值 }，重复字段保留数组结构，缺失路径不出现
export function projectRecord(record, projection) {
  const projected = {};
  for (const path of projection) {
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
