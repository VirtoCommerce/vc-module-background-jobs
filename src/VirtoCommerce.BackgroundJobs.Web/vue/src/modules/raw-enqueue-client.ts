import { BackgroundJobsAdminClient, type EnqueueOptions } from "../api_client/virtocommerce.backgroundjobs";
import { buildEnqueueBody } from "./enqueue-body";

/**
 * The generated client plus enqueueRaw, which sends the payload text as typed through window.fetch (the generated
 * client's default) and parses the response with the generated processEnqueue.
 */
export class RawEnqueueClient extends BackgroundJobsAdminClient {
  enqueueRaw(name: string, payloadText: string | undefined, options: EnqueueOptions): Promise<string> {
    const options_: RequestInit = {
      body: buildEnqueueBody(name, payloadText, options),
      method: "POST",
      headers: {
        "Content-Type": "application/json-patch+json",
        Accept: "application/json",
      },
    };

    return this.transformOptions(options_)
      .then((transformed) => window.fetch("/api/background-jobs/enqueue", transformed))
      .then((response) => this.processEnqueue(response));
  }
}
