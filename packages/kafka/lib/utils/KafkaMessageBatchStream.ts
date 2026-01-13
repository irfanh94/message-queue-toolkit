import { Transform } from 'node:stream'
import { clearTimeout } from 'node:timers'

// Topic and partition are required for the stream to work properly
type MessageWithTopicAndPartition = { topic: string; partition: number }

export type KafkaMessageBatchOptions = {
  batchSize: number
  timeoutMilliseconds: number
}

export type MessageBatch<TMessage> = { topic: string; partition: number; messages: TMessage[] }
export type OnMessageBatchCallback<TMessage> = (batch: MessageBatch<TMessage>) => Promise<void>

/**
 * Collects messages in batches based on provided batchSize and flushes them when messages amount or timeout is reached.
 *
 * This implementation uses Transform stream which properly handles backpressure by design.
 * When the downstream consumer is slow, the stream will automatically pause accepting new messages
 * until the consumer catches up, preventing memory leaks and OOM errors.
 */
export class KafkaMessageBatchStream<
  TMessage extends MessageWithTopicAndPartition,
> extends Transform {
  private readonly onBatch: OnMessageBatchCallback<TMessage>
  private readonly batchSize: number
  private readonly timeout: number

  private readonly currentBatchPerTopicPartition: Record<string, TMessage[]>
  private readonly batchTimeoutPerTopicPartition: Record<string, NodeJS.Timeout | undefined>

  constructor(
    onBatch: OnMessageBatchCallback<TMessage>,
    options: { batchSize: number; timeoutMilliseconds: number },
  ) {
    super({ objectMode: true })
    this.onBatch = onBatch
    this.batchSize = options.batchSize
    this.timeout = options.timeoutMilliseconds
    this.currentBatchPerTopicPartition = {}
    this.batchTimeoutPerTopicPartition = {}
  }

  override async _transform(
    message: TMessage,
    _encoding: BufferEncoding,
    callback: (error?: Error | null | undefined) => void,
  ) {
    try {
      const key = getTopicPartitionKey(message.topic, message.partition)

      if (!this.currentBatchPerTopicPartition[key]) {
        this.currentBatchPerTopicPartition[key] = []
      }

      this.currentBatchPerTopicPartition[key].push(message)

      if (this.currentBatchPerTopicPartition[key].length >= this.batchSize) {
        await this.flushCurrentBatchMessages(message.topic, message.partition)
      } else if (!this.batchTimeoutPerTopicPartition[key]) {
        this.batchTimeoutPerTopicPartition[key] = setTimeout(() => {
          void this.flushCurrentBatchMessages(message.topic, message.partition)
        }, this.timeout)
      }
    } catch (error) {
      callback(error as Error)
      return
    }

    callback()
  }

  // Flush all remaining batches when stream is closing
  override async _flush(callback: (error?: Error | null | undefined) => void) {
    try {
      await this.flushAllBatches()
      callback()
    } catch (error) {
      callback(error as Error)
    }
  }

  private async flushAllBatches() {
    const keys: string[] = Object.keys(this.currentBatchPerTopicPartition)

    for (const key of keys) {
      const { topic, partition } = splitTopicPartitionKey(key)
      await this.flushCurrentBatchMessages(topic, partition)
    }
  }

  private async flushCurrentBatchMessages(topic: string, partition: number) {
    const key = getTopicPartitionKey(topic, partition)

    const timeout = this.batchTimeoutPerTopicPartition[key]

    if (timeout) {
      clearTimeout(timeout)
      this.batchTimeoutPerTopicPartition[key] = undefined
    }

    const messages = this.currentBatchPerTopicPartition[key] ?? []

    if (messages.length === 0) return

    try {
      // Push the batch downstream
      await this.onBatch({ topic, partition, messages })
    } finally {
      this.currentBatchPerTopicPartition[key] = []
    }
  }
}

const getTopicPartitionKey = (topic: string, partition: number): string => `${topic}:${partition}`
const splitTopicPartitionKey = (key: string): { topic: string; partition: number } => {
  const [topic, partition] = key.split(':')
  /* v8 ignore start */
  if (!topic || !partition) throw new Error('Invalid topic-partition key format')
  /* v8 ignore stop */

  return { topic, partition: Number.parseInt(partition, 10) }
}
