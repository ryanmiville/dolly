import dolly
import gleeunit

import dolly/consumer
import dolly/dispatcher/demand_dispatcher.{type DemandDispatcher}
import dolly/processor
import dolly/producer
import dolly/subscription
import gleam/list
import gleeunit/should
import redux/erlang/process.{type Subject}

pub fn main() {
  gleeunit.main()
}

fn counter(
  initial_state: Int,
) -> producer.Builder(Int, DemandDispatcher(Int), Int) {
  let handle_demand = fn(state, demand) {
    let events = list.range(state, state + demand - 1)
    producer.Next(events, state + demand)
  }
  producer.new(initial_state)
  |> producer.handle_demand(handle_demand)
}

fn forwarder(receiver: Subject(List(Int))) -> consumer.Builder(Int, Int) {
  consumer.new(0)
  |> consumer.handle_events(fn(state, events) {
    process.send(receiver, events)
    consumer.Continue(state)
  })
}

fn doubler(
  receiver: Subject(List(Int)),
) -> processor.Builder(Int, DemandDispatcher(Int), Int, Int) {
  processor.new(0)
  |> processor.handle_events(fn(state, events) {
    process.send(receiver, events)
    let events = list.flat_map(events, fn(event) { [event, event] })
    producer.Next(events, state)
  })
}

fn pass_through(
  receiver: Subject(List(Int)),
) -> processor.Builder(Int, DemandDispatcher(Int), Int, Int) {
  processor.new(0)
  |> processor.handle_events(fn(state, events) {
    process.send(receiver, events)
    producer.Next(events, state)
  })
}

fn discarder(
  receiver: Subject(List(Int)),
) -> processor.Builder(Int, DemandDispatcher(nothing), Int, nothing) {
  processor.new(0)
  |> processor.handle_events(fn(state, events) {
    process.send(receiver, events)
    producer.Next([], state)
  })
}

fn sleeper(receiver: Subject(List(Int))) -> consumer.Builder(Int, Int) {
  consumer.new(0)
  |> consumer.handle_events(fn(state, events) {
    process.send(receiver, events)
    process.sleep_forever()
    consumer.Continue(state)
  })
}

fn assert_received(subject: Subject(a), expected: a, timeout: Int) {
  process.receive(subject, timeout)
  |> should.equal(Ok(expected))
}

fn assert_not_received(subject: Subject(a), not_expected: a, timeout: Int) {
  process.receive(subject, timeout)
  |> should.not_equal(Ok(not_expected))
}

fn assert_received_eventually(subject: Subject(a), expected: a, timeout: Int) {
  receive_eventually(subject, expected, timeout)
  |> should.equal(Ok(expected))
}

pub fn producer_to_consumer_default_demand_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let sub = subscription.to(prod)

  let events_subject = process.new_subject()
  let assert Ok(consumer) =
    forwarder(events_subject)
    |> consumer.add_subscription(sub)
    |> consumer.start

  consumer |> dolly.subscribe(to: sub)

  let batch = list.range(0, 499)
  assert_received(events_subject, batch, 20)

  let batch = list.range(500, 999)
  assert_received(events_subject, batch, 20)
}

pub fn producer_to_consumer_80_percent_min_demand_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let sub =
    subscription.to(prod)
    |> subscription.min_demand(80)
    |> subscription.max_demand(100)

  let events_subject = process.new_subject()
  let assert Ok(consumer) =
    forwarder(events_subject)
    |> consumer.add_subscription(sub)
    |> consumer.start

  dolly.subscribe(from: consumer, to: sub)

  let batch = list.range(0, 19)
  assert_received(events_subject, batch, 20)

  let batch = list.range(20, 39)
  assert_received(events_subject, batch, 20)

  let batch = list.range(1000, 1019)
  assert_received_eventually(events_subject, batch, 20)
}

pub fn producer_to_consumer_20_percent_min_demand_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let sub =
    subscription.to(prod)
    |> subscription.min_demand(20)
    |> subscription.max_demand(100)

  let events_subject = process.new_subject()
  let assert Ok(consumer) =
    forwarder(events_subject)
    |> consumer.add_subscription(sub)
    |> consumer.start

  dolly.subscribe(from: consumer, to: sub)

  let batch = list.range(0, 79)
  assert_received(events_subject, batch, 20)

  let batch = list.range(80, 99)
  assert_received(events_subject, batch, 20)

  let batch = list.range(100, 179)
  assert_received(events_subject, batch, 20)

  let batch = list.range(180, 259)
  assert_received(events_subject, batch, 20)

  let batch = list.range(260, 279)
  assert_received(events_subject, batch, 20)
}

pub fn producer_to_consumer_0_min_1_max_demand_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let sub =
    subscription.to(prod)
    |> subscription.min_demand(0)
    |> subscription.max_demand(1)

  let events_subject = process.new_subject()
  let assert Ok(consumer) =
    forwarder(events_subject)
    |> consumer.add_subscription(sub)
    |> consumer.start

  dolly.subscribe(consumer, sub)

  assert_received(events_subject, [0], 20)

  assert_received(events_subject, [1], 20)

  assert_received(events_subject, [2], 20)
}

// // pub fn producer_to_consumer_broadcast_demand_test() {
// //   logging.log(
// //     logging.Warning,
// //     "TODO: producer_to_consumer: with shared (broadcast) demand",
// //   )
// //   logging.log(
// //     logging.Warning,
// //     "TODO: producer_to_consumer: with shared (broadcast) demand and synchronizer subscriber",
// //   )
// // }

pub fn producer_to_processor_to_consumer_80_percent_min_demand_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let doubler_subject = process.new_subject()
  let assert Ok(doubler) = doubler(doubler_subject) |> processor.start

  let consumer_subject = process.new_subject()
  let assert Ok(consumer) = forwarder(consumer_subject) |> consumer.start

  let prod_sub =
    subscription.to(prod)
    |> subscription.min_demand(80)
    |> subscription.max_demand(100)

  let proc_sub =
    subscription.to(doubler |> processor.as_producer)
    |> subscription.min_demand(50)
    |> subscription.max_demand(100)

  dolly.subscribe(consumer, proc_sub)
  dolly.subscribe(processor.as_consumer(doubler), prod_sub)

  let batch = list.range(0, 19)
  assert_received(doubler_subject, batch, 20)

  let batch = doubled_range(0, 19)
  assert_received(consumer_subject, batch, 20)

  let batch = doubled_range(20, 39)
  assert_received(consumer_subject, batch, 20)

  let batch = list.range(100, 119)
  assert_received_eventually(doubler_subject, batch, 100)

  let batch = list.flat_map(list.range(120, 124), fn(event) { [event, event] })
  assert_received_eventually(consumer_subject, batch, 100)

  let batch = list.flat_map(list.range(125, 139), fn(event) { [event, event] })
  assert_received_eventually(consumer_subject, batch, 100)
}

pub fn producer_to_processor_to_consumer_20_percent_min_demand_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let doubler_subject = process.new_subject()
  let assert Ok(doubler) = doubler(doubler_subject) |> processor.start

  let consumer_subject = process.new_subject()
  let assert Ok(consumer) = forwarder(consumer_subject) |> consumer.start

  let prod_sub =
    subscription.to(prod)
    |> subscription.min_demand(20)
    |> subscription.max_demand(100)

  let proc_sub =
    subscription.to(doubler |> processor.as_producer)
    |> subscription.min_demand(50)
    |> subscription.max_demand(100)

  dolly.subscribe(consumer, proc_sub)
  dolly.subscribe(processor.as_consumer(doubler), prod_sub)

  let batch = list.range(0, 79)
  assert_received(doubler_subject, batch, 20)

  let batch = doubled_range(0, 24)
  assert_received(consumer_subject, batch, 20)

  let batch = doubled_range(25, 49)
  assert_received(consumer_subject, batch, 20)

  let batch = doubled_range(50, 74)
  assert_received(consumer_subject, batch, 20)

  let batch = list.range(100, 179)
  assert_received_eventually(doubler_subject, batch, 100)
}

pub fn producer_to_processor_to_consumer_80_percent_min_demand_late_subscription_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let doubler_subject = process.new_subject()
  let assert Ok(doubler) = doubler(doubler_subject) |> processor.start

  let consumer_subject = process.new_subject()
  let assert Ok(consumer) = forwarder(consumer_subject) |> consumer.start

  let prod_sub =
    subscription.to(prod)
    |> subscription.min_demand(80)
    |> subscription.max_demand(100)

  let proc_sub =
    subscription.to(doubler |> processor.as_producer)
    |> subscription.min_demand(50)
    |> subscription.max_demand(100)

  // consumer first
  dolly.subscribe(consumer, proc_sub)
  dolly.subscribe(processor.as_consumer(doubler), prod_sub)

  let batch = list.range(0, 19)
  assert_received(doubler_subject, batch, 20)

  let batch = doubled_range(0, 19)
  assert_received(consumer_subject, batch, 20)

  let batch = doubled_range(20, 39)
  assert_received(consumer_subject, batch, 20)

  let batch = list.range(100, 119)
  assert_received_eventually(doubler_subject, batch, 100)

  let batch = list.flat_map(list.range(120, 124), fn(event) { [event, event] })
  assert_received_eventually(consumer_subject, batch, 100)

  let batch = list.flat_map(list.range(125, 139), fn(event) { [event, event] })
  assert_received_eventually(consumer_subject, batch, 100)
}

pub fn producer_to_processor_to_consumer_20_percent_min_demand_late_subscription_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let doubler_subject = process.new_subject()
  let assert Ok(doubler) = doubler(doubler_subject) |> processor.start

  let consumer_subject = process.new_subject()
  let assert Ok(consumer) = forwarder(consumer_subject) |> consumer.start

  let prod_sub =
    subscription.to(prod)
    |> subscription.min_demand(20)
    |> subscription.max_demand(100)

  let proc_sub =
    subscription.to(doubler |> processor.as_producer)
    |> subscription.min_demand(50)
    |> subscription.max_demand(100)

  // consumer first
  dolly.subscribe(consumer, proc_sub)
  dolly.subscribe(processor.as_consumer(doubler), prod_sub)

  let batch = list.range(0, 79)
  assert_received(doubler_subject, batch, 20)

  let batch = doubled_range(0, 24)
  assert_received(consumer_subject, batch, 20)

  let batch = doubled_range(25, 49)
  assert_received(consumer_subject, batch, 20)

  let batch = doubled_range(50, 74)
  assert_received(consumer_subject, batch, 20)

  let batch = list.range(100, 179)
  assert_received_eventually(doubler_subject, batch, 100)
}

pub fn producer_to_processor_to_consumer_stops_asking_when_consumer_stops_asking_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let pass_through_subject = process.new_subject()
  let assert Ok(pass_through) =
    pass_through(pass_through_subject) |> processor.start

  let sleeper_subject = process.new_subject()
  let assert Ok(sleeper) = sleeper(sleeper_subject) |> consumer.start

  let counter_sub =
    subscription.to(prod)
    |> subscription.min_demand(8)
    |> subscription.max_demand(10)

  let pass_through_sub =
    subscription.to(processor.as_producer(pass_through))
    |> subscription.min_demand(5)
    |> subscription.max_demand(10)

  dolly.subscribe(processor.as_consumer(pass_through), counter_sub)
  dolly.subscribe(sleeper, pass_through_sub)

  assert_received(pass_through_subject, [0, 1], 20)
  assert_received(sleeper_subject, [0, 1], 20)
  assert_received(pass_through_subject, [2, 3], 20)
  assert_received(pass_through_subject, [4, 5], 20)
  assert_received(pass_through_subject, [6, 7], 20)
  assert_received(pass_through_subject, [8, 9], 20)
  assert_not_received(sleeper_subject, [2, 3], 20)
  assert_not_received(pass_through_subject, [10, 11], 20)
}

pub fn producer_to_processor_to_consumer_keeps_emitting_even_when_discarded_test() {
  let assert Ok(prod) = counter(0) |> producer.start

  let discarder_subject = process.new_subject()
  let assert Ok(discarder) = discarder(discarder_subject) |> processor.start

  let forwarder_subject = process.new_subject()
  let assert Ok(forwarder) = forwarder(forwarder_subject) |> consumer.start

  let discarder_sub =
    subscription.to(processor.as_producer(discarder))
    |> subscription.min_demand(50)
    |> subscription.max_demand(100)

  let counter_sub =
    subscription.to(prod)
    |> subscription.min_demand(80)
    |> subscription.max_demand(100)

  dolly.subscribe(forwarder, discarder_sub)
  dolly.subscribe(processor.as_consumer(discarder), counter_sub)

  assert_received(discarder_subject, list.range(0, 19), 20)
  assert_received_eventually(discarder_subject, list.range(100, 119), 100)
  assert_received_eventually(discarder_subject, list.range(1000, 1019), 100)
}

// // pub fn single_test() {
// //   let prod = single(0)
// //   let subject = process.new_subject()
// //   let cons = forwarder(subject)
// //   subscription.from(cons) |> subscription.to(prod)

// //   assert_received(subject, [0], 20)
// //   assert_received(subject, [1], 20)
// //   assert_received(subject, [3], 20)
// // }

// // pub fn producer_to_processor_to_consumer_with_broadcast_demand_test() {
// //   logging.log(
// //     logging.Warning,
// //     "TODO: producer_to_processor_to_consumer: with broadcast demand",
// //   )
// //   logging.log(
// //     logging.Warning,
// //     "TODO: producer_to_processor_to_consumer: with broadcast demand and synchronizer subscriber",
// //   )
// // }

// // pub fn producer_to_processor_to_consumer_queued_events_with_lost_producer_test() {
// //   logging.log(
// //     logging.Warning,
// //     "TODO: producer_to_processor_to_consumer: queued events with lost producer",
// //   )
// // }

fn doubled_range(start: Int, end: Int) -> List(Int) {
  list.flat_map(list.range(start, end), fn(event) { [event, event] })
}

@external(erlang, "dolly_test_ffi", "receive_eventually")
fn receive_eventually(
  subject: Subject(a),
  expected: a,
  timeout: Int,
) -> Result(a, Nil)
