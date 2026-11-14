import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const parquetThrift = require('parquetjs-lite/gen-nodejs/parquet_types.js');
const parquetUtil = require('parquetjs-lite/lib/util.js');

export const MAGIC = 'PAR1';
export const TRAILER_SIZE = 8; // 4 字节页脚长度 + 4 字节魔数

// 把 thrift 解出的 i64（node-int64 对象或 number）转成安全整数
export function toSafeNumber(value, what = '整数') {
  if (value === null || value === undefined) return value;
  const number = Number(value);
  if (!Number.isSafeInteger(number)) throw new Error(`${what} 超出安全整数范围`);
  return number;
}

// 解析对象尾部 8 字节，返回页脚（FileMetaData）的字节范围
export function readTrailer(trailer) {
  if (trailer.length !== TRAILER_SIZE) throw new Error('尾部截断，对象不是完整的 Parquet 文件');
  if (trailer.subarray(4).toString() !== MAGIC) throw new Error('尾部魔数不符，对象不是 Parquet 文件');
  return { metadataSize: trailer.readUInt32LE(0) };
}

export function decodeMetadata(buffer) {
  const metadata = new parquetThrift.FileMetaData();
  parquetUtil.decodeThrift(metadata, buffer);
  if (metadata.encryption_algorithm || metadata.footer_signing_key_metadata) {
    throw new Error('不支持加密的 Parquet 对象');
  }
  return metadata;
}

export function encodeMetadata(metadata) {
  return parquetUtil.serializeThrift(metadata);
}

export function encodeFooter(metadata) {
  const body = encodeMetadata(metadata);
  const footer = Buffer.alloc(body.length + TRAILER_SIZE);
  body.copy(footer);
  footer.writeUInt32LE(body.length, body.length);
  footer.write(MAGIC, body.length + 4);
  return footer;
}

// 把扁平的 schema 元素列表（先序遍历）解析成树
function parseSchemaTree(elements) {
  let index = 0;
  const parseNode = () => {
    const element = elements[index];
    index += 1;
    const node = { element, children: [] };
    const count = element.num_children || 0;
    for (let i = 0; i < count; i += 1) node.children.push(parseNode());
    return node;
  };
  const root = parseNode();
  if (index !== elements.length) throw new Error('schema 元素数量与结构不一致');
  return root;
}

// 列出全部叶子列：path 为点分路径（如 lanes.name），element 为 SchemaElement
export function schemaLeaves(schemaElements) {
  const root = parseSchemaTree(schemaElements);
  const leaves = [];
  const walk = (node, segments) => {
    for (const child of node.children) {
      const path = [...segments, child.element.name];
      if (child.children.length === 0) {
        leaves.push({ path: path.join('.'), segments: path, element: child.element });
      } else {
        walk(child, path);
      }
    }
  };
  walk(root, []);
  return leaves;
}

// 按投影裁剪 schema：保留被投影叶子及其全部祖先节点。
// 祖先的 repetition_type 原样保留，因此叶子的定义/重复层级不变。
export function pruneSchema(schemaElements, projection) {
  const wanted = projection instanceof Set ? projection : new Set(projection);
  const root = parseSchemaTree(schemaElements);
  const cloneElement = (element, numChildren) => {
    const clone = Object.assign(new parquetThrift.SchemaElement(), element);
    if (numChildren === undefined) delete clone.num_children;
    else clone.num_children = numChildren;
    return clone;
  };
  const prune = (node, segments) => {
    const kept = [];
    for (const child of node.children) {
      const path = [...segments, child.element.name];
      if (child.children.length === 0) {
        if (wanted.has(path.join('.'))) kept.push({ element: cloneElement(child.element), children: [] });
      } else {
        const pruned = prune(child, path);
        if (pruned) kept.push(pruned);
      }
    }
    if (kept.length === 0) return null;
    return { element: cloneElement(node.element, kept.length), children: kept };
  };
  const pruned = prune(root, []);
  if (!pruned) throw new Error('投影没有命中任何列');
  const flat = [];
  const flatten = node => {
    flat.push(node.element);
    node.children.forEach(flatten);
  };
  flatten(pruned);
  return flat;
}

// 列块在对象内的字节范围 [start, end)，覆盖字典页与全部数据页
export function chunkRange(columnChunk) {
  const meta = columnChunk.meta_data;
  const dataOffset = toSafeNumber(meta.data_page_offset, '数据页偏移');
  const dictionaryOffset = meta.dictionary_page_offset === null || meta.dictionary_page_offset === undefined
    ? null : toSafeNumber(meta.dictionary_page_offset, '字典页偏移');
  const start = dictionaryOffset === null ? dataOffset : Math.min(dictionaryOffset, dataOffset);
  const end = start + toSafeNumber(meta.total_compressed_size, '列块压缩大小');
  return { start, end };
}

// 平移列块偏移量（装配新文件时使用），丢弃指向旧文件的页索引位置
export function shiftColumnChunk(columnChunk, delta) {
  const source = columnChunk.meta_data;
  const meta = new parquetThrift.ColumnMetaData();
  meta.type = source.type;
  meta.encodings = source.encodings;
  meta.path_in_schema = source.path_in_schema;
  meta.codec = source.codec;
  meta.num_values = source.num_values;
  meta.total_uncompressed_size = source.total_uncompressed_size;
  meta.total_compressed_size = source.total_compressed_size;
  if (source.key_value_metadata) meta.key_value_metadata = source.key_value_metadata;
  if (source.statistics) meta.statistics = source.statistics;
  meta.data_page_offset = toSafeNumber(source.data_page_offset, '数据页偏移') + delta;
  if (source.dictionary_page_offset !== null && source.dictionary_page_offset !== undefined) {
    meta.dictionary_page_offset = toSafeNumber(source.dictionary_page_offset, '字典页偏移') + delta;
  }
  const chunk = new parquetThrift.ColumnChunk();
  chunk.file_offset = meta.dictionary_page_offset === undefined
    ? meta.data_page_offset : Math.min(meta.dictionary_page_offset, meta.data_page_offset);
  chunk.meta_data = meta;
  return chunk;
}

// 基于源行组与已平移的列块构造新行组元数据
export function buildRowGroup(sourceRowGroup, columns, firstChunkOffset) {
  const rowGroup = new parquetThrift.RowGroup();
  rowGroup.columns = columns;
  rowGroup.num_rows = sourceRowGroup.num_rows;
  rowGroup.total_byte_size = columns.reduce(
    (sum, column) => sum + toSafeNumber(column.meta_data.total_uncompressed_size, '列块未压缩大小'), 0);
  if (sourceRowGroup.total_compressed_size !== null && sourceRowGroup.total_compressed_size !== undefined) {
    rowGroup.total_compressed_size = columns.reduce(
      (sum, column) => sum + toSafeNumber(column.meta_data.total_compressed_size, '列块压缩大小'), 0);
  }
  rowGroup.file_offset = firstChunkOffset === undefined ? 0 : firstChunkOffset;
  if (sourceRowGroup.ordinal !== null && sourceRowGroup.ordinal !== undefined) {
    rowGroup.ordinal = sourceRowGroup.ordinal;
  }
  return rowGroup;
}

// 装配输出文件的 FileMetaData；column_orders 随列集合变化而丢弃
export function buildFileMetadata({ source, schemaElements, rowGroups, numRows }) {
  const metadata = new parquetThrift.FileMetaData();
  metadata.version = source.version;
  metadata.schema = schemaElements;
  metadata.num_rows = numRows;
  metadata.row_groups = rowGroups;
  metadata.key_value_metadata = source.key_value_metadata || [];
  if (source.created_by) metadata.created_by = source.created_by;
  return metadata;
}

const enumName = (klass, value) => parquetUtil.getThriftEnum(klass, value);

// 叶子列的比较类别：utf8 / bytes / number / bigint / boolean，无法安全排序时返回 null
function leafKind(leaf) {
  const element = leaf.element || leaf;
  const type = enumName(parquetThrift.Type, element.type);
  const converted = element.converted_type === null || element.converted_type === undefined
    ? null : enumName(parquetThrift.ConvertedType, element.converted_type);
  const logicalString = element.logicalType && element.logicalType.STRING;
  switch (type) {
    case 'BYTE_ARRAY':
      if (converted === 'UTF8' || logicalString) return 'utf8';
      return converted ? null : 'bytes';
    case 'FIXED_LEN_BYTE_ARRAY':
      return converted ? null : 'bytes';
    case 'BOOLEAN': return 'boolean';
    case 'INT32': return converted && !['INT_32', 'INT_16', 'INT_8', 'UINT_32', 'UINT_16', 'UINT_8', 'DATE', 'TIME_MILLIS'].includes(converted) ? null : 'number';
    case 'FLOAT': return converted ? null : 'number';
    case 'DOUBLE': return converted ? null : 'number';
    case 'INT64': return converted && !['INT_64', 'UINT_64', 'TIME_MICROS', 'TIME_MILLIS', 'TIMESTAMP_MILLIS', 'TIMESTAMP_MICROS'].includes(converted) ? null : 'bigint';
    default: return null;
  }
}

const asBytes = raw => (Buffer.isBuffer(raw) ? raw : Buffer.from(String(raw), 'utf8'));

// 把统计值原始字节解码为可比较的字面量 { kind, value }；类型不支持时返回 null
export function decodeStatValue(raw, leaf) {
  const kind = leafKind(leaf);
  if (!kind) return null;
  const buffer = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
  try {
    switch (kind) {
      case 'utf8':
      case 'bytes': return { kind: 'bytes', value: asBytes(raw) };
      case 'boolean': return { kind: 'number', value: (buffer[0] & 1) ? 1 : 0 };
      case 'number': {
        const type = enumName(parquetThrift.Type, (leaf.element || leaf).type);
        if (type === 'INT32') return { kind: 'number', value: buffer.readInt32LE(0) };
        if (type === 'FLOAT') return { kind: 'number', value: buffer.readFloatLE(0) };
        return { kind: 'number', value: buffer.readDoubleLE(0) };
      }
      case 'bigint': return { kind: 'bigint', value: buffer.readBigInt64LE(0) };
      default: return null;
    }
  } catch {
    return null;
  }
}

// 把筛选条件里的字符串字面量转换为与统计值相同的比较形态
export function literalFor(term, leaf) {
  const kind = leafKind(leaf);
  if (!kind) return null;
  try {
    switch (kind) {
      case 'utf8':
      case 'bytes': return { kind: 'bytes', value: Buffer.from(term.value, 'utf8') };
      case 'boolean': {
        if (!/^(true|false)$/i.test(term.value)) return null;
        return { kind: 'number', value: term.value.toLowerCase() === 'true' ? 1 : 0 };
      }
      case 'number': {
        const value = Number(term.value);
        return Number.isFinite(value) ? { kind: 'number', value } : null;
      }
      case 'bigint': return { kind: 'bigint', value: BigInt(term.value) };
      default: return null;
    }
  } catch {
    return null;
  }
}

// 无符号字节序比较（Parquet UTF8 统计值的排序约定），数值按大小比较
export function compareLiteral(left, right) {
  if (left.kind !== right.kind) throw new Error('字面量与统计值类型不一致');
  if (left.kind === 'bytes') return Buffer.compare(left.value, right.value);
  if (left.value < right.value) return -1;
  if (left.value > right.value) return 1;
  return 0;
}

// 依据列块统计值评估单个行组是否可能命中筛选条件。
// 原则：统计值缺失、被截断或类型不支持时一律保留（多取不漏取）。
export function evaluateRowGroup(rowGroup, filter, leavesByPath) {
  const reasons = [];
  for (const term of filter) {
    const leaf = leavesByPath.get(term.path);
    if (!leaf) throw new Error(`筛选字段不存在: ${term.path}`);
    const chunk = (rowGroup.columns || []).find(
      column => column.meta_data && column.meta_data.path_in_schema.join('.') === term.path);
    if (!chunk) {
      reasons.push({ term: term.path, outcome: 'keep', reason: 'column-chunk-missing' });
      continue;
    }
    const statistics = chunk.meta_data.statistics;
    if (!statistics) {
      reasons.push({ term: term.path, outcome: 'keep', reason: 'statistics-missing' });
      continue;
    }
    const minRaw = statistics.min_value ?? statistics.min;
    const maxRaw = statistics.max_value ?? statistics.max;
    if (minRaw === null || minRaw === undefined || maxRaw === null || maxRaw === undefined) {
      reasons.push({ term: term.path, outcome: 'keep', reason: 'statistics-without-min-max' });
      continue;
    }
    const min = decodeStatValue(minRaw, leaf);
    const max = decodeStatValue(maxRaw, leaf);
    const literal = literalFor(term, leaf);
    if (!min || !max || !literal) {
      reasons.push({ term: term.path, outcome: 'keep', reason: 'statistics-type-unsupported' });
      continue;
    }
    const minExact = statistics.is_min_value_exact !== false;
    const maxExact = statistics.is_max_value_exact !== false;
    if (compareLiteral(literal, min) < 0 && minExact) {
      reasons.push({ term: term.path, outcome: 'skip', reason: 'below-min' });
      return { decision: 'skip', reasons };
    }
    if (compareLiteral(literal, max) > 0 && maxExact) {
      reasons.push({ term: term.path, outcome: 'skip', reason: 'above-max' });
      return { decision: 'skip', reasons };
    }
    reasons.push({
      term: term.path, outcome: 'keep',
      reason: minExact && maxExact ? 'within-min-max' : 'statistics-not-exact'
    });
  }
  return { decision: 'keep', reasons };
}

// 记录级筛选：嵌套路径逐层解析，重复字段任一元素命中即视为命中，空值不匹配等值条件
export function matchesFilter(record, filter, leavesByPath) {
  return filter.every(term => {
    const leaf = leavesByPath.get(term.path);
    if (!leaf) throw new Error(`筛选字段不存在: ${term.path}`);
    const literal = literalFor(term, leaf);
    if (!literal) throw new Error(`筛选字段类型不支持: ${term.path}`);
    const segments = term.path.split('.');
    const resolve = (value, depth, found) => {
      if (value === null || value === undefined) return found;
      if (Array.isArray(value)) {
        for (const item of value) resolve(item, depth, found);
        return found;
      }
      if (depth === segments.length) {
        found.push(value);
        return found;
      }
      return resolve(value[segments[depth]], depth + 1, found);
    };
    const candidates = resolve(record, 0, []);
    return candidates.some(value => compareLiteral(wrapValue(value, literal.kind), literal) === 0);
  });
}

function wrapValue(value, kind) {
  if (kind === 'bytes') return { kind, value: Buffer.from(String(value), 'utf8') };
  if (kind === 'bigint') return { kind, value: BigInt(value) };
  return { kind: 'number', value: Number(value) };
}
