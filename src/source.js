import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { digest } from './archive.js';

// 本地范围服务：模拟支持 HTTP Range 与对象版本校验(etag/If-Match)的远端。
// options.onRequest(info) 在每次 /object 请求处理前调用（可用于中途换版）；
// options.failFrom 表示从第几个 /object 请求起返回 500（故障注入），
// 返回对象上的 recover() 可恢复服务。stats 记录请求数、传输字节与已服务范围。
export async function serveObject(filename, options = {}) {
  let bytes = await readFile(filename);
  const stats = { requests: 0, transmitted: 0, ranges: [] };
  let failFrom = options.failFrom ?? Infinity;
  const server = http.createServer((request, response) => {
    response.setHeader('connection', 'close');
    if (request.url === '/stats') {
      response.setHeader('content-type', 'application/json');
      return response.end(JSON.stringify(stats));
    }
    if (request.url !== '/object') { response.writeHead(404); return response.end(); }
    stats.requests += 1;
    const index = stats.requests;
    if (options.onRequest) {
      options.onRequest({ index, method: request.method, range: request.headers.range || null });
    }
    if (index >= failFrom) { response.writeHead(500); return response.end(); }
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
      if (request.method !== 'HEAD') stats.ranges.push({ start, end });
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
    failFrom: value => { failFrom = value; },
    recover: () => { failFrom = Infinity; },
    close: () => new Promise(resolve => server.close(resolve)) };
}
