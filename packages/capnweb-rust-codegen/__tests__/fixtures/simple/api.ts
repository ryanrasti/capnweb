declare class RpcTarget {}

export type Status = "active" | "disabled";

export interface User {
  displayName: string;
  nickname?: string;
  active: boolean;
  status: Status;
}

export declare class Counter extends RpcTarget {
  increment(by: number): number;
}

export interface Directory {
  primaryCounter: Counter;
}

export declare class Api extends RpcTarget {
  get currentUser(): User;
  echoUser(user: User): Promise<User>;
  makeCounter(initial: number): Counter;
  directory(): Directory;
}
