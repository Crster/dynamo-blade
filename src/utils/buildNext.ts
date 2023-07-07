export function decodeNext(next: string) {
  if (next) {
    return JSON.parse(Buffer.from(next, "base64").toString());
  }
}

export function encodeNext(next: any) {
  if (next) {
    return Buffer.from(JSON.stringify(next)).toString("base64");
  }
}
