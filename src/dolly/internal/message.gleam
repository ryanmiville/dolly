import redux/erlang/process.{type Subject}

pub type Producer(event) {
  Ask(demand: Int, consumer: Subject(Consumer(event)))
  ProducerSubscribe(consumer: Subject(Consumer(event)), demand: Int)
  ProducerUnsubscribe(consumer: Subject(Consumer(event)))
  ConsumerDown(process.Down)
}

pub type Processor(in, out) {
  ConsumerMsg(Consumer(in))
  ProducerMsg(Producer(out))
}

pub type Consumer(event) {
  NewEvents(events: List(event), from: Subject(Producer(event)))
  ConsumerSubscribe(
    producer: Subject(Producer(event)),
    min_demand: Int,
    max_demand: Int,
  )
  ConsumerUnsubscribe(producer: Subject(Producer(event)))
  ProducerDown(process.Down)
}
