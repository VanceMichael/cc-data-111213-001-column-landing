import parquet from 'parquetjs-lite';
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
