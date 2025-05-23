import redux/erlang/process.{type Subject}

pub type Producer(event) {
  Ask(demand: Int, consumer: Subject(Consumer(event)))
  ProducerSubscribe(consumer: Subject(Consumer(event)), demand: Int)
  ProducerUnsubscribe(consumer: Subject(Consumer(event)))
  ConsumerDown(process.Down)
  Accumulate
  Forward
}

pub type Processor(in, out) {
  ConsumerMsg(Consumer(in))
  ProducerMsg(Producer(out))
}

pub type Consumer(event) {
  NewEvents(events: List(event), from: Subject(Producer(event)))
  ConsumerSubscribe(
    producer: Subject(Producer(event)),
    cancel: Cancel,
    min_demand: Int,
    max_demand: Int,
  )
  ConsumerUnsubscribe(producer: Subject(Producer(event)))
  ProducerDown(process.Down)
}

// TODO move
//In case of exits, the same reason is used to exit the consumer.
pub type Cancel {
  /// the consumer exits when the producer cancels or exits.
  Permanent
  /// then consumer exits only if reason is not :normal, :shutdown, or {:shutdown, reason}
  Transient
  /// the consumer never exits
  Temporary
}
