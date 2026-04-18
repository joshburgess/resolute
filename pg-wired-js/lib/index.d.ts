import {
  Connection,
  Pool,
  Pipeline,
  RowStream,
  Notifications,
  Notification,
  CancelToken,
  ConnectOptions,
  FieldInfo,
  QueryResult,
} from '../index.js';

export {
  Connection,
  Pool,
  Pipeline,
  RowStream,
  Notifications,
  Notification,
  CancelToken,
  ConnectOptions,
  FieldInfo,
  QueryResult,
};

export declare function connect(
  options: ConnectOptions | string,
): Promise<Connection>;
export declare function createPool(
  options: ConnectOptions | string,
  size: number,
): Promise<Pool>;
export declare function parsePgUrl(url: string): ConnectOptions;

export interface PgErrorPayload {
  kind: 'pg' | 'io' | 'protocol' | 'closed';
  code?: string;
  message: string;
  severity?: string;
  detail?: string;
  hint?: string;
  position?: string;
}

export declare class PgError extends Error {
  readonly name: 'PgError';
  readonly kind: PgErrorPayload['kind'];
  readonly code?: string;
  readonly severity?: string;
  readonly detail?: string;
  readonly hint?: string;
  readonly position?: string;
}

declare module '../index.js' {
  interface RowStream {
    [Symbol.asyncIterator](): AsyncIterator<Array<string | null | undefined>>;
  }
  interface Notifications {
    [Symbol.asyncIterator](): AsyncIterator<Notification>;
  }
}
