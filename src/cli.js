import { readFile, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import assert from 'node:assert/strict';
import { Archive } from './archive.js';
import { inspect, readRecords, projectRecord, sample } from './domain.js';
import { collect } from './collect.js';
import { serveObject } from './source.js';
import { openRemote, fetchRange } from './rangeClient.js';

const [command = 'help', ...args] = process.argv.slice(2);

function parseOptions(list) {
  const positional = [];
  const options = { where: [] };
  for (let i = 0; i < list.length; i += 1) {
    const arg = list[i];
    if (arg === '--select') options.select = list[++i].split(',').map(s => s.trim()).filter(Boolean);
    else if (arg === '--where') {
      const eq = (list[++i] || '').indexOf('=');
      if (eq <= 0) throw new Error('--where 需要 字段=值 形式');
      const raw = list[i];
      options.where.push({ path: raw.slice(0, eq), op: 'eq', value: raw.slice(eq + 1) });
    }
    else if (arg === '--workdir') options.workdir = list[++i];
    else if (arg === '--concurrency') options.concurrency = Number(list[++i]);
    else positional.push(arg);
  }
  return { positional, options };
}

async function run() {
  if (command === 'help') {
    console.log(JSON.stringify({ usage: 'npm start -- <命令> [参数]',
      commands: [
        'inspect <资料路径>',
        'capture <资料路径> [归档目录]                整文件模式：保存来源字节',
        'collect <对象地址> <归档目录> [--select 列,列] [--where 列=值] [--workdir 目录] [--concurrency 数]',
        'list [归档目录]',
        'demo'
      ] }, null, 2));
  } else if (command === 'inspect') {
    const [file] = args;
    if (!file) throw new Error('需要资料路径');
    console.log(JSON.stringify(await inspect(resolve(file)), null, 2));
  } else if (command === 'capture') {
    const [file, directory = '.local/archive'] = args;
    if (!file) throw new Error('需要资料路径');
    const inventory = await inspect(resolve(file));
    const receipt = await new Archive(directory).accept(await readFile(file),
      { source: resolve(file), inventory });
    console.log(JSON.stringify(receipt, null, 2));
  } else if (command === 'collect') {
    const { positional, options } = parseOptions(args);
    const [url, directory = '.local/archive'] = positional;
    if (!url) throw new Error('需要对象地址');
    const { receipt } = await collect({
      url,
      archiveDir: directory,
      workdir: options.workdir,
      projection: options.select || null,
      filter: options.where,
      concurrency: options.concurrency || 4
    });
    console.log(JSON.stringify(receipt, null, 2));
  } else if (command === 'list') {
    const [directory = '.local/archive'] = args;
    console.log(JSON.stringify(await new Archive(directory).receipts(), null, 2));
  } else if (command === 'demo') {
    await demo();
  } else { throw new Error('支持 inspect、capture、collect、list、demo'); }
}

// 对照演示：整文件模式保留；按需领取经本地范围服务完成，
// 覆盖空值、字典编码与领取中途换版，最后与整文件基准逐条比对。
async function demo() {
  const temporary = await mkdtemp(join(tmpdir(), 'intake-'));
  try {
    const inventory = await inspect(resolve(sample));
    const archive = new Archive(join(temporary, 'archive'));
    const receipt = await archive.accept(await readFile(sample), { source: sample, inventory });
    const wholeFile = { sha256: receipt.sha256, verifiedBytes: (await archive.read(receipt.sha256)).length };

    const updated = await readFile('fixtures/rates-daily-v2.parquet');
    let swapped = false;
    const source = await serveObject('fixtures/rates-daily.parquet', {
      onRequest: ({ index }) => {
        if (index === 4 && !swapped) { swapped = true; source.replace(updated); }
      }
    });
    try {
      const projection = ['date', 'supplier', 'rate', 'lanes.name'];
      const filter = [{ path: 'date', op: 'eq', value: '2026-11-02' }];
      const result = await collect({
        url: `${source.url}/object`,
        archiveDir: join(temporary, 'archive'),
        workdir: join(temporary, 'work'),
        projection, filter, concurrency: 2
      });
      const credential = result.receipt.metadata;

      // 整文件模式读取同一版本作为基准
      const remote = await openRemote(`${source.url}/object`);
      const whole = await fetchRange(remote, 0, remote.size - 1);
      const wholePath = join(temporary, 'whole.parquet');
      await writeFile(wholePath, whole);
      const reference = await readRecords(wholePath, filter, projection);
      const selective = (await readFile(result.recordsPath, 'utf8'))
        .split('\n').filter(Boolean).map(line => projectRecord(JSON.parse(line), projection));
      assert.deepStrictEqual(selective, reference);

      console.log(JSON.stringify({
        wholeFile,
        selective: {
          etag: credential.source.etag,
          swappedMidway: swapped,
          restarts: credential.plan.restarts,
          bytesRead: credential.transfer.totalBytes,
          objectBytes: credential.transfer.objectBytes,
          rowGroups: credential.summary,
          records: credential.output.records.rows
        },
        comparison: { consistent: true, rows: selective.length, sample: selective }
      }, null, 2));
    } finally { await source.close(); }
  } finally { await rm(temporary, { recursive: true, force: true }); }
}

run().catch(error => { console.error(error.message); process.exitCode = 1; });
