export type Codec = "Json" | "Binary";

export interface Batch {
  max_bytes: number;
  max_count: number;
}

export interface Encoding {
  codec: Codec;
}
