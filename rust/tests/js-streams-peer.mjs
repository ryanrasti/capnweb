// The JavaScript side of tests/js_streams.rs: Cap'n Web (CAPNWEB_JS, default this repository's
// build) over newline-delimited stdio, checking the Rust peer's ReadableStream and
// WritableStream values. Prints "ok <check>" per check on stderr; exits 1 at the first failure.
const { RpcSession } = await import(process.env.CAPNWEB_JS ?? "../../dist/index.js");

class StdioTransport {
  #queue = [];
  #waiter;
  #buffer = "";

  constructor() {
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", chunk => {
      this.#buffer += chunk;
      for (;;) {
        const newline = this.#buffer.indexOf("\n");
        if (newline < 0) break;
        const message = this.#buffer.slice(0, newline);
        this.#buffer = this.#buffer.slice(newline + 1);
        if (this.#waiter) {
          const waiter = this.#waiter;
          this.#waiter = undefined;
          waiter.resolve(message);
        } else {
          this.#queue.push(message);
        }
      }
    });
    process.stdin.on("end", () => this.#waiter?.reject(new Error("Rust peer closed stdin")));
  }

  send(message) {
    process.stdout.write(`${message}\n`);
  }

  receive() {
    if (this.#queue.length > 0) return Promise.resolve(this.#queue.shift());
    return new Promise((resolve, reject) => {
      this.#waiter = { resolve, reject };
    });
  }
}

const api = new RpcSession(new StdioTransport()).getRemoteMain();
const check = (name, ok, detail = "") => {
  if (!ok) {
    console.error(`FAILED ${name} ${detail}`);
    process.exit(1);
  }
  console.error(`ok ${name}`);
};
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// a ReadableStream's bytes, all of them, in order
{
  const stream = await api.pattern(300, 7001);
  let total = 0;
  let intact = true;
  let bytes = true;
  for await (const chunk of stream) {
    bytes &&= chunk instanceof Uint8Array;
    for (const byte of chunk) {
      if (byte !== total % 251) intact = false;
      total++;
    }
  }
  check("readable's chunks are Uint8Arrays", bytes);
  check("readable carries every byte in order", total === 300 * 7001 && intact, `${total}`);
}

// a WritableStream's chunks, written in order, closed
{
  const stream = await api.sink();
  const writer = stream.getWriter();
  for (let i = 0; i < 200; i++) await writer.write(new Uint8Array(10_000).fill(i % 256));
  await writer.close();
  check("writable takes every chunk in order", (await api.received()) === "200x10000 in order");
}

// a slow reader: what the Rust side reads ahead stays bounded (its flow control)
{
  const stream = await api.endless();
  const reader = stream.getReader();
  await reader.read();
  await sleep(500);
  const produced = await api.produced();
  check("a slow reader bounds what is read ahead", produced < 4 * 1024 * 1024, `${produced} bytes`);
  // cancelled: the source is dropped, not read on
  await reader.cancel();
  let dropped = false;
  for (let i = 0; i < 100 && !dropped; i++) {
    dropped = await api.endlessDropped();
    if (!dropped) await sleep(20);
  }
  check("cancelling a readable drops its source", dropped);
}

// a slow sink: what this side writes ahead of it stays bounded (its flow control)
{
  const stream = await api.slowSink();
  const writer = stream.getWriter();
  const chunk = new Uint8Array(64 * 1024);
  let written = 0;
  let worst = 0;
  while (written < 32 * 1024 * 1024 && performance.now() < 30_000) {
    await writer.ready;
    writer.write(chunk);
    written += chunk.length;
    if (written % (1024 * 1024) === 0) worst = Math.max(worst, written - (await api.slowReceived()));
    if (written >= 4 * 1024 * 1024) break;
  }
  check("a slow sink bounds what is written ahead", worst < 3 * 1024 * 1024, `${worst} bytes ahead`);
  await writer.abort();
}

// a read error: the stream errors with it
{
  const stream = await api.failing();
  let message = "";
  try {
    for await (const _ of stream) {}
  } catch (error) {
    message = String(error?.message ?? error);
  }
  check("a read error errors the readable", message.includes("boom"), message);
}

process.exit(0);
