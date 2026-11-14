import { createReadStream } from 'node:fs';
import { mkdir, open, readFile, rename, readdir, rm } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

export function digest(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

// 原子写入：临时文件 + fsync + 改名，崩溃不会留下半份内容
export async function writeAtomicFile(path, bytes) {
  const temporary = `${path}.${randomUUID()}.tmp`;
  const handle = await open(temporary, 'wx', 0o600);
  try { await handle.writeFile(bytes); await handle.sync(); }
  finally { await handle.close(); }
  try { await rename(temporary, path); }
  finally { await rm(temporary, { force: true }); }
}

export class Archive {
  constructor(directory) { this.directory = resolve(directory); }
  async initialize() {
    await mkdir(join(this.directory, 'objects'), { recursive: true });
    await mkdir(join(this.directory, 'receipts'), { recursive: true });
  }
  async writeAtomic(path, bytes) { return writeAtomicFile(path, bytes); }
  // 只保存对象字节，返回内容摘要
  async store(bytes) {
    await this.initialize();
    const sha256 = digest(bytes);
    await this.writeAtomic(join(this.directory, 'objects', sha256), bytes);
    return { sha256, size: bytes.length };
  }
  // 流式保存较大文件，逐块计算摘要，内存占用与文件大小无关
  async storeFile(path) {
    await this.initialize();
    const temporary = join(this.directory, 'objects', `.incoming-${randomUUID()}`);
    const handle = await open(temporary, 'wx', 0o600);
    const hash = createHash('sha256');
    let size = 0;
    try {
      for await (const chunk of createReadStream(path, { highWaterMark: 1 << 20 })) {
        hash.update(chunk);
        size += chunk.length;
        await handle.write(chunk);
      }
      await handle.sync();
    } finally { await handle.close(); }
    const sha256 = hash.digest('hex');
    try { await rename(temporary, join(this.directory, 'objects', sha256)); }
    finally { await rm(temporary, { force: true }); }
    return { sha256, size };
  }
  // 登记一条回执/凭据
  async record(metadata) {
    await this.initialize();
    const receipt = { id: randomUUID(), observedAt: new Date().toISOString(), ...metadata };
    await this.writeAtomic(join(this.directory, 'receipts', `${receipt.id}.json`),
      JSON.stringify(receipt, null, 2));
    return receipt;
  }
  async accept(bytes, metadata) {
    const { sha256, size } = await this.store(bytes);
    return this.record({ sha256, size, metadata });
  }
  async receipts() {
    await this.initialize();
    const files = (await readdir(join(this.directory, 'receipts')))
      .filter(name => name.endsWith('.json')).sort();
    return Promise.all(files.map(name => readFile(join(this.directory, 'receipts', name), 'utf8').then(JSON.parse)));
  }
  objectPath(sha256) {
    if (!/^[a-f0-9]{64}$/.test(sha256)) throw new Error('对象摘要格式不正确');
    return join(this.directory, 'objects', sha256);
  }
  async read(sha256) {
    const bytes = await readFile(this.objectPath(sha256));
    if (digest(bytes) !== sha256) throw new Error('对象完整性校验失败');
    return bytes;
  }
}
