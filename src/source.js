import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { digest } from './archive.js';

export async function serveObject(filename) {
  let bytes = await readFile(filename);
  const stats = { requests: 0, transmitted: 0 };
  const server = http.createServer((request, response) => {
    if (request.url === '/stats') {
      response.setHeader('content-type', 'application/json');
      return response.end(JSON.stringify(stats));
    }
    if (request.url !== '/object') { response.writeHead(404); return response.end(); }
    stats.requests += 1;
    const etag = `"${digest(bytes)}"`;
    response.setHeader('etag', etag);
    response.setHeader('accept-ranges', 'bytes');
    if (request.headers['if-match'] && request.headers['if-match'] !== etag) {
      response.writeHead(412); return response.end();
    }
    let start = 0, end = bytes.length - 1;
    if (request.headers.range) {
      const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range);
      if (!match) { response.writeHead(416); return response.end(); }
      start = Number(match[1]); end = Math.min(Number(match[2]), end);
      if (start > end || start >= bytes.length) { response.writeHead(416); return response.end(); }
      response.statusCode = 206;
      response.setHeader('content-range', `bytes ${start}-${end}/${bytes.length}`);
    }
    const content = bytes.subarray(start, end + 1);
    response.setHeader('content-length', content.length);
    if (request.method === 'HEAD') return response.end();
    stats.transmitted += content.length;
    response.end(content);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  return { url: `http://127.0.0.1:${server.address().port}`, stats,
    replace: value => { bytes = Buffer.from(value); },
    close: () => new Promise(resolve => server.close(resolve)) };
}
