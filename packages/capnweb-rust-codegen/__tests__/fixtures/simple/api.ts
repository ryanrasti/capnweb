declare class RpcTarget {}

export type Status = "active" | "disabled";

export interface User {
  displayName: string;
  nickname?: string;
  active: boolean;
  status: Status;
}

export type Identifier = string | number;

export type Formatter = (value: string) => string | Promise<string>;

export type Event =
  | { kind: "created"; user: User }
  | { kind: "deleted"; id: string };

export declare class Counter extends RpcTarget {
  increment(by: number): number;
}

export interface Directory {
  primaryCounter: Counter;
}

export interface TreeNode {
  value: string;
  children: TreeNode[];
}

export declare class Api extends RpcTarget {
  get currentUser(): User;
  echoUser(user: User): Promise<User>;
  echoEvent(event: Event): Event;
  echoIdentifier(identifier: Identifier): Identifier;
  echoBigInt(value: bigint): bigint;
  nothing(): void;
  echoArray(value: readonly string[]): string[];
  echoTuple(value: readonly [string, number, boolean?]): [string, number, boolean?];
  echoRecord(value: Record<string, string>): Record<string, string>;
  echoTree(value: TreeNode): TreeNode;
  acceptDeferred(value: Promise<string>): Promise<string>;
  echoDynamic(value: unknown): unknown;
  echoBytes(value: Uint8Array): Uint8Array;
  echoDate(value: Date): Date;
  echoStream(value: ReadableStream<string>): ReadableStream<string>;
  applyFormatter(formatter: Formatter, value: string): Promise<string>;
  formatter(): Formatter;
  makeCounter(initial: number): Counter;
  directory(): Directory;
}
