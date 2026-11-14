import { mkdir as ensureFixtureDirectory, readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import parquet from 'parquetjs-lite';
import {
  MAGIC, TRAILER_SIZE, readTrailer, decodeMetadata, encodeFooter,
  chunkRange, shiftColumnChunk, buildRowGroup, buildFileMetadata
} from '../src/parquet/meta.js';

const require = createRequire(import.meta.url);
const parquetThrift = require('parquetjs-lite/gen-nodejs/parquet_types.js');
const parquetUtil = require('parquetjs-lite/lib/util.js');
const rle = require('parquetjs-lite/lib/codec/rle.js');

await ensureFixtureDirectory('fixtures', { recursive: true });

// —— 既有样例：保持不动，既有查看/归档行为以其为准 ——
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

// —— 每日明细样例：多行组、空值、字典编码、缺统计值的行组 ——
const dailySchema = new parquet.ParquetSchema({
  date: { type: 'UTF8' },
  supplier: { type: 'UTF8' },
  rate: { type: 'DOUBLE', optional: true },
  note: { type: 'UTF8', optional: true, statistics: false }, // 该列整列不写统计值
  lanes: { repeated: true, fields: { name: { type: 'UTF8' }, enabled: { type: 'BOOLEAN' } } }
});

const dailyRows = rateOverride => [
  { date: '2026-11-01', supplier: '甲', rate: 1.2, note: '常规', lanes: [{ name: '东线', enabled: true }] },
  { date: '2026-11-01', supplier: '乙', rate: 0.9, note: '常规', lanes: [] },
  { date: '2026-11-02', supplier: '甲', note: '缺报价', lanes: [{ name: '西线', enabled: false }] }, // rate 空值
  { date: '2026-11-02', supplier: '乙', rate: 2.0, lanes: [] },                                     // note 空值
  { date: '2026-11-02', supplier: '丙', rate: rateOverride, note: '边界', lanes: [{ name: '东线', enabled: true }, { name: '西线', enabled: true }] },
  { date: '2026-11-03', supplier: '甲', rate: 4.1, note: '跨日', lanes: [] },
  { date: '2026-11-04', supplier: '乙', rate: 1.1, note: '', lanes: [] },
  { date: '2026-11-04', supplier: '丙', rate: 2.2, note: '', lanes: [{ name: '南线', enabled: true }] }
];

// 手工构造字典编码列块：字典页(PLAIN) + 数据页(PLAIN_DICTIONARY 索引)
function craftDictionaryChunk(values, baseOffset) {
  const dictionary = [...new Set(values)];
  const indices = values.map(value => dictionary.indexOf(value));
  const bitWidth = Math.max(1, Math.ceil(Math.log2(dictionary.length)));

  const dictionaryBody = Buffer.concat(dictionary.map(value => {
    const bytes = Buffer.from(value, 'utf8');
    const length = Buffer.alloc(4);
    length.writeUInt32LE(bytes.length);
    return Buffer.concat([length, bytes]);
  }));
  const dictionaryHeader = new parquetThrift.PageHeader();
  dictionaryHeader.type = parquetThrift.PageType.DICTIONARY_PAGE;
  dictionaryHeader.uncompressed_page_size = dictionaryBody.length;
  dictionaryHeader.compressed_page_size = dictionaryBody.length;
  dictionaryHeader.dictionary_page_header = new parquetThrift.DictionaryPageHeader();
  dictionaryHeader.dictionary_page_header.num_values = dictionary.length;
  dictionaryHeader.dictionary_page_header.encoding = parquetThrift.Encoding.PLAIN;
  const dictionaryPage = Buffer.concat([parquetUtil.serializeThrift(dictionaryHeader), dictionaryBody]);

  const valuesBody = Buffer.concat([
    Buffer.from([bitWidth]),
    rle.encodeValues('INT32', [...indices], { bitWidth, disableEnvelope: true })
  ]);
  const dataHeader = new parquetThrift.PageHeader();
  dataHeader.type = parquetThrift.PageType.DATA_PAGE;
  dataHeader.uncompressed_page_size = valuesBody.length;
  dataHeader.compressed_page_size = valuesBody.length;
  dataHeader.data_page_header = new parquetThrift.DataPageHeader();
  dataHeader.data_page_header.num_values = values.length;
  dataHeader.data_page_header.encoding = parquetThrift.Encoding.PLAIN_DICTIONARY;
  dataHeader.data_page_header.definition_level_encoding = parquetThrift.Encoding.RLE;
  dataHeader.data_page_header.repetition_level_encoding = parquetThrift.Encoding.RLE;
  const dataPage = Buffer.concat([parquetUtil.serializeThrift(dataHeader), valuesBody]);

  const bytes = Buffer.concat([dictionaryPage, dataPage]);
  const meta = new parquetThrift.ColumnMetaData();
  meta.type = parquetThrift.Type.BYTE_ARRAY;
  meta.encodings = [parquetThrift.Encoding.PLAIN_DICTIONARY, parquetThrift.Encoding.RLE, parquetThrift.Encoding.PLAIN];
  meta.path_in_schema = ['supplier'];
  meta.codec = parquetThrift.CompressionCodec.UNCOMPRESSED;
  meta.num_values = values.length;
  meta.total_uncompressed_size = bytes.length;
  meta.total_compressed_size = bytes.length;
  meta.dictionary_page_offset = baseOffset;
  meta.data_page_offset = baseOffset + dictionaryPage.length;
  const sorted = [...dictionary].sort((a, b) => Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8')));
  meta.statistics = new parquetThrift.Statistics({
    min_value: Buffer.from(sorted[0], 'utf8'),
    max_value: Buffer.from(sorted[sorted.length - 1], 'utf8'),
    null_count: 0,
    distinct_count: dictionary.length
  });
  return { bytes, meta };
}

// 写出基础文件后重组：supplier 列换成字典编码列块，并抹掉第 2 个行组 date 列的统计值
async function buildDaily(path, rows) {
  const temporary = `${path}.base`;
  const writer = await parquet.ParquetWriter.openFile(dailySchema, temporary, { rowGroupSize: 2 });
  for (const row of rows) await writer.appendRow(row);
  await writer.close();

  const base = await readFile(temporary);
  const { metadataSize } = readTrailer(base.subarray(base.length - TRAILER_SIZE));
  const metadata = decodeMetadata(base.subarray(base.length - TRAILER_SIZE - metadataSize, base.length - TRAILER_SIZE));

  const parts = [Buffer.from(MAGIC)];
  let offset = MAGIC.length;
  const rowGroups = [];
  metadata.row_groups.forEach((rowGroup, groupIndex) => {
    const suppliers = rows.slice(groupIndex * 2, groupIndex * 2 + 2).map(row => row.supplier);
    const columns = [];
    let firstOffset;
    for (const column of rowGroup.columns) {
      const columnPath = column.meta_data.path_in_schema.join('.');
      let chunkBytes, chunkMeta;
      if (columnPath === 'supplier') {
        const crafted = craftDictionaryChunk(suppliers, offset);
        chunkBytes = crafted.bytes;
        chunkMeta = crafted.meta;
      } else {
        const range = chunkRange(column);
        chunkBytes = base.subarray(range.start, range.end);
        chunkMeta = shiftColumnChunk(column, offset - range.start).meta_data;
      }
      const chunk = new parquetThrift.ColumnChunk();
      chunk.file_offset = offset;
      chunk.meta_data = chunkMeta;
      columns.push(chunk);
      if (firstOffset === undefined) firstOffset = offset;
      parts.push(chunkBytes);
      offset += chunkBytes.length;
    }
    rowGroups.push(buildRowGroup(rowGroup, columns, firstOffset));
  });

  // 第 2 个行组的 date 列不提供统计值：筛选时该行组必须被保留（多取不漏取）
  const stripped = rowGroups[1].columns.find(
    column => column.meta_data.path_in_schema.join('.') === 'date');
  stripped.meta_data.statistics = null;

  const footer = encodeFooter(buildFileMetadata({
    source: metadata, schemaElements: metadata.schema, rowGroups, numRows: rows.length
  }));
  await writeFile(path, Buffer.concat([...parts, footer]));
  const { rm } = await import('node:fs/promises');
  await rm(temporary, { force: true });
}

await buildDaily('fixtures/rates-daily.parquet', dailyRows(3.5));
// 第二版对象：同一批日期，仅一张 2026-11-02 的报价被更新（用于中途换版对照）
await buildDaily('fixtures/rates-daily-v2.parquet', dailyRows(3.8));
console.log('fixtures ready');
