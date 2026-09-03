import { RpcTarget, type RpcStub } from "capnweb";

/** A plain pass-by-value record. */
export interface User {
  id: string;
  displayName: string;
  nickname?: string;
  nullableBio: string | null;
  explicitUndefined: string | undefined;
  active: boolean;
  score: number;
  loginCount: bigint;
  tags: string[];
  readonly aliases: readonly string[];
  location: readonly [latitude: number, longitude: number];
  metadata: Record<string, string>;
}

/** A discriminated pass-by-value union. */
export type UserEvent =
  | { kind: "created"; user: User }
  | { kind: "renamed"; userId: string; displayName: string }
  | { kind: "deleted"; userId: string; reason?: string };

/** The byte containers currently understood by Cap'n Web. */
export interface ByteValues {
  arrayBuffer: ArrayBuffer;
  dataView: DataView;
  int8: Int8Array;
  uint8: Uint8Array;
  uint8Clamped: Uint8ClampedArray;
  int16: Int16Array;
  uint16: Uint16Array;
  int32: Int32Array;
  uint32: Uint32Array;
  bigint64: BigInt64Array;
  biguint64: BigUint64Array;
  float32: Float32Array;
  float64: Float64Array;
}

/** A stateful object passed by reference. */
export declare class Counter extends RpcTarget {
  get value(): number;
  increment(by?: number): number;
  reset(): void;
}

/** A callable value is also passed by reference. */
export type Formatter = (
  value: string,
  options?: { uppercase?: boolean },
) => string | Promise<string>;

/** A multi-method callback target. */
export declare class EventSink extends RpcTarget {
  event(value: UserEvent): void | Promise<void>;
  done(): void | Promise<void>;
}

export interface CounterDirectory {
  primary: Counter;
  all: Counter[];
  byName: Record<string, Counter>;
}

/**
 * A deliberately broad API used to agree on generated Rust shapes.
 *
 * Blob, Request, and Response are intentionally omitted for now. Map, Set,
 * RegExp, application-defined by-value classes, and cyclic objects are not
 * currently supported by Cap'n Web.
 */
export declare class KitchenSinkApi extends RpcTarget {
  // Primitive and structural values.
  nothing(): void;
  undefinedValue(): undefined;
  nullableText(value: string | null): string | null;
  echoBoolean(value: boolean): boolean;
  echoNumber(value: number): number;
  echoBigInt(value: bigint): bigint;
  echoString(value: string): string;
  echoUser(value: User): User;
  echoUsers(values: readonly User[]): User[];
  echoTuple(value: readonly [string, number, boolean?]): [string, number, boolean?];
  echoEvent(value: UserEvent): UserEvent;
  countLabels(...labels: string[]): number;

  // Built-in pass-by-value types retained in the initial Rust design.
  echoBytes(value: ByteValues): ByteValues;
  echoDate(value: Date): Date;
  echoUrl(value: URL): URL;
  echoHeaders(value: Headers): Headers;
  echoError(value: Error): Error;

  // Promise values may be forwarded without awaiting them locally.
  acceptDeferred(value: Promise<string>): Promise<string>;

  // Targets and functions passed by reference.
  makeCounter(initial: number): Counter;
  useCounter(counter: RpcStub<Counter>, by: number): Promise<number>;
  counterDirectory(): CounterDirectory;
  format(formatter: RpcStub<Formatter>, value: string): Promise<string>;
  makeFormatter(prefix: string): Formatter;
  watch(sink: RpcStub<EventSink>): Promise<void>;

  // Streams carry arbitrary RPC-compatible chunks and are unidirectional.
  downloadUsers(): ReadableStream<User>;
  uploadUsers(source: ReadableStream<User>): Promise<number>;
  writeUsers(destination: WritableStream<User>): Promise<void>;
  copyUsers(
    source: ReadableStream<User>,
    destination: WritableStream<User>,
  ): Promise<number>;
}
