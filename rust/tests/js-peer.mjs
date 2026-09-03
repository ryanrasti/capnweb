import { RpcSession, RpcTarget } from "../../dist/index.js";

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
    process.stdin.on("end", () => {
      this.#waiter?.reject(new Error("Rust peer closed stdin"));
    });
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

let calledByRustResolve;
const calledByRust = new Promise(resolve => {
  calledByRustResolve = resolve;
});

class JavaScriptCalculator extends RpcTarget {
  triple(value) {
    calledByRustResolve();
    return value * 3;
  }
}

const session = new RpcSession(new StdioTransport(), new JavaScriptCalculator());
const api = session.getRemoteMain();

// JavaScript calls Rust, forwarding one unresolved result into another call.
const sum = api.add(20, 1);
const result = await api.double(sum);
if (result !== 42) {
  throw new Error(`expected Rust to return 42, got ${result}`);
}

// Keep the process alive until Rust has also called the JavaScript target.
await calledByRust;
process.exit(0);
