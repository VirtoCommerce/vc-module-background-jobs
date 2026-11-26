import type { EnqueueOptions } from "../api_client/virtocommerce.backgroundjobs";

/**
 * Builds the enqueue request body as text, splicing the payload JSON in exactly as typed: a parse/stringify round trip
 * would round large numbers in the browser. A blank payload sends none.
 */
export function buildEnqueueBody(name: string, payloadText: string | undefined, options: EnqueueOptions): string {
  const payload = payloadText?.trim();
  const payloadMember = payload ? `,"payload":${payload}` : "";

  return `{"name":${JSON.stringify(name)},"options":${JSON.stringify(options)}${payloadMember}}`;
}
