// 带对象版本钉住的 HTTP 范围客户端。
// 同一轮领取内的所有请求都携带 If-Match: <开启时观察到的 etag>，
// 保证页脚、列块、字典页始终来自同一对象版本；远端在领取期间换版会
// 收到 412（或响应 etag 不一致），由调用方整体重开，绝不混版拼接。

export class VersionChangedError extends Error {
  constructor(message) {
    super(message);
    this.name = 'VersionChangedError';
  }
}

export class RemoteObjectError extends Error {
  constructor(message, { retryable = false } = {}) {
    super(message);
    this.name = 'RemoteObjectError';
    this.retryable = retryable;
  }
}

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

// 发现对象当前版本与大小；远端必须支持版本校验与字节范围
export async function openRemote(url) {
  let response;
  try {
    response = await fetch(url, { method: 'HEAD' });
  } catch (error) {
    throw new RemoteObjectError(`无法连接远端对象: ${error.message}`, { retryable: true });
  }
  if (!response.ok) throw new RemoteObjectError(`远端对象不可用(状态 ${response.status})`);
  const etag = response.headers.get('etag');
  if (!etag) throw new RemoteObjectError('远端未提供对象版本标识(etag)，无法保证范围一致');
  const acceptRanges = response.headers.get('accept-ranges') || '';
  if (!acceptRanges.includes('bytes')) throw new RemoteObjectError('远端不支持字节范围请求');
  const size = Number(response.headers.get('content-length'));
  if (!Number.isSafeInteger(size) || size < 0) throw new RemoteObjectError('远端未提供有效的对象大小');
  return { url, etag, size };
}

// 读取闭区间 [start, end]，校验版本与内容范围；短暂故障按次数重试
export async function fetchRange(remote, start, end, { retries = 3, backoffMs = 50 } = {}) {
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end < start) {
    throw new RemoteObjectError(`非法范围 ${start}-${end}`);
  }
  let attempt = 0;
  for (;;) {
    attempt += 1;
    try {
      return await fetchRangeOnce(remote, start, end);
    } catch (error) {
      if (error instanceof VersionChangedError) throw error;
      const retryable = error instanceof RemoteObjectError ? error.retryable : true;
      if (!retryable || attempt > retries) throw error;
      await wait(backoffMs * attempt);
    }
  }
}

async function fetchRangeOnce(remote, start, end) {
  let response;
  try {
    response = await fetch(remote.url, {
      headers: { range: `bytes=${start}-${end}`, 'if-match': remote.etag }
    });
  } catch (error) {
    throw new RemoteObjectError(`范围请求失败: ${error.message}`, { retryable: true });
  }
  if (response.status === 412) {
    throw new VersionChangedError(`对象 ${remote.url} 在领取期间被替换(版本 ${remote.etag} 已失效)`);
  }
  if (response.status === 416) throw new RemoteObjectError(`范围 ${start}-${end} 超出对象大小`);
  if (response.status >= 500) throw new RemoteObjectError(`远端暂时故障(状态 ${response.status})`, { retryable: true });
  if (response.status !== 206) throw new RemoteObjectError(`远端未按范围响应(状态 ${response.status})`);
  const etag = response.headers.get('etag');
  if (etag && etag !== remote.etag) {
    throw new VersionChangedError(`对象 ${remote.url} 在领取期间被替换(响应版本 ${etag})`);
  }
  const contentRange = response.headers.get('content-range') || '';
  const match = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(contentRange);
  if (!match || Number(match[1]) !== start || Number(match[2]) !== end) {
    throw new RemoteObjectError(`远端返回的内容范围不符: ${contentRange || '缺失'}`);
  }
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length !== end - start + 1) {
    throw new RemoteObjectError(`范围响应被截断(期望 ${end - start + 1} 字节,实收 ${bytes.length})`, { retryable: true });
  }
  return bytes;
}

// 有界并发执行器：任意一项失败即停止派发新任务
export async function runPool(items, concurrency, worker) {
  const results = new Array(items.length);
  let next = 0;
  let firstError = null;
  const run = async () => {
    while (next < items.length && !firstError) {
      const index = next;
      next += 1;
      try {
        results[index] = await worker(items[index], index);
      } catch (error) {
        firstError = firstError || error;
      }
    }
  };
  const lanes = Math.max(1, Math.min(concurrency, items.length));
  await Promise.all(Array.from({ length: lanes }, run));
  if (firstError) throw firstError;
  return results;
}
