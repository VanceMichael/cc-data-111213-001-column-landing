import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { inspect, sample } from '../src/domain.js';
import { serveObject } from '../src/source.js';

test('既有资料查看与本地来源符合领域约定', async () => {
  const result = await inspect(sample);
  assert.equal(result.rows, 4);
  assert.equal(result.preview[0].lanes[0].name, '东线');
  assert.equal(result.preview[1].rate, undefined);
  const source = await serveObject(sample);
  try {
    const response = await fetch(`${source.url}/object`, { headers: { range: 'bytes=0-3' } });
    assert.equal(response.status, 206);
    assert.equal(await response.text(), 'PAR1');
    const etag = response.headers.get('etag');
    source.replace(Buffer.from('PAR1changed'));
    assert.equal((await fetch(`${source.url}/object`, { headers: { 'if-match': etag } })).status, 412);
  } finally { await source.close(); }
});
