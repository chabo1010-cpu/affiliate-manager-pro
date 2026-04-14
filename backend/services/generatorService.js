import { createGeneratorPublishingEntry } from './publisherService.js';

export function enqueueGeneratorPost(input = {}) {
  return createGeneratorPublishingEntry(input);
}
