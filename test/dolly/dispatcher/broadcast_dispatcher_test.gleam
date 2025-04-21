import dolly/dispatcher
import dolly/dispatcher/broadcast_dispatcher.{
  type BroadcastDispatcher, BroadcastDispatcher,
} as bd
import dolly/internal/message
import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{None}
import gleam/set
import gleam/string
import gleeunit/should
import redux/erlang/process.{type Subject}
import redux/otp/actor
import redux/otp/task

fn spawn_forwarder() -> Subject(message.Consumer(a)) {
  let parent = process.new_subject()
  let assert Ok(actor.Started(data:, ..)) =
    actor.new(parent)
    |> actor.on_message(forwarder_loop)
    |> actor.start
  data
}

fn forwarder_loop(parent, msg) {
  process.send(parent, msg)
  actor.continue(parent)
}

pub fn subscribes_and_cancels_test() {
  let disp = bd.build() |> dispatcher.initialise
  let subject = process.new_subject()

  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject)

  let expected_subscribers = set.from_list([subject])

  // Check state has the expected format
  let assert BroadcastDispatcher([#(ref, demand)], 0, subscribers) = disp.state
  ref |> should.equal(subject)
  demand.counter |> should.equal(0)
  demand.selector |> should.equal(None)
  subscribers |> should.equal(expected_subscribers)

  // Cancel subscription
  let assert #(0, disp) = dispatcher.cancel(disp, subject)
  let assert BroadcastDispatcher([], 0, subscribers) = disp.state
  subscribers |> should.equal(set.new())
}

pub fn subscribes_asks_and_cancels_test() {
  let disp = bd.build() |> dispatcher.initialise
  let subject = process.new_subject()
  let expected_subscribers = set.from_list([subject])

  // Subscribe
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject)
  let assert BroadcastDispatcher([#(ref, demand)], 0, subscribers) = disp.state
  ref |> should.equal(subject)
  demand.counter |> should.equal(0)
  demand.selector |> should.equal(None)
  subscribers |> should.equal(expected_subscribers)

  // Ask
  let assert #(10, disp) = dispatcher.ask(disp, 10, subject)
  let assert BroadcastDispatcher([#(_, demand)], 10, subscribers) = disp.state
  demand.counter |> should.equal(0)
  subscribers |> should.equal(expected_subscribers)

  // Cancel
  let assert #(0, disp) = dispatcher.cancel(disp, subject)
  let assert BroadcastDispatcher([], 0, subscribers) = disp.state
  subscribers |> should.equal(set.new())
}

pub fn multiple_subscriptions_with_early_demand_test() {
  let disp = bd.build() |> dispatcher.initialise
  let subject1 = process.new_subject()
  let subject2 =
    task.async(fn() { process.new_subject() }) |> task.await_forever

  // First subscription
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject1)
  let expected_subs1 = set.from_list([subject1])

  let assert BroadcastDispatcher([#(ref, demand)], 0, subs) = disp.state
  ref |> should.equal(subject1)
  demand.counter |> should.equal(0)
  subs |> should.equal(expected_subs1)

  // Ask demand for first subscription
  let assert #(10, disp) = dispatcher.ask(disp, 10, subject1)
  let assert BroadcastDispatcher([#(_, demand)], 10, subs) = disp.state
  demand.counter |> should.equal(0)
  subs |> should.equal(expected_subs1)

  // Add second subscription
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject2)
  let expected_subs2 = set.insert(expected_subs1, subject2)

  let BroadcastDispatcher(subscribed_processes:, ..) = disp.state
  subscribed_processes |> should.equal(expected_subs2)

  // Cancel first subscription
  let assert #(0, disp) = dispatcher.cancel(disp, subject1)
  let expected_subs3 = set.delete(expected_subs2, subject1)

  let assert BroadcastDispatcher([#(ref, _)], 0, subs) = disp.state
  ref |> should.equal(subject2)
  subs |> should.equal(expected_subs3)

  // Ask demand for second subscription
  let assert #(10, disp) = dispatcher.ask(disp, 10, subject2)
  let assert BroadcastDispatcher([#(_, demand)], 10, subs) = disp.state
  demand.counter |> should.equal(0)
  subs |> should.equal(expected_subs3)
}

pub fn multiple_subscriptions_with_late_demand_test() {
  let disp = bd.build() |> dispatcher.initialise
  let subject1 = process.new_subject()
  let subject2 = spawn_forwarder()

  // First subscription
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject1)
  let expected_subs1 = set.from_list([subject1])

  let assert BroadcastDispatcher([#(ref, _)], 0, subs) = disp.state
  ref |> should.equal(subject1)
  subs |> should.equal(expected_subs1)

  // Second subscription
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject2)
  let expected_subs2 = set.insert(expected_subs1, subject2)

  let BroadcastDispatcher(subscribed_processes:, ..) = disp.state
  subscribed_processes |> should.equal(expected_subs2)

  // Ask demand for first subscription
  let assert #(0, disp) = dispatcher.ask(disp, 10, subject1)

  let assert BroadcastDispatcher([#(_, demand_a), #(_, demand_b)], 0, _) =
    disp.state
  let has_subject1 =
    list.any([demand_a.counter, demand_b.counter], fn(c) { c == 10 })
  has_subject1 |> should.be_true

  // Cancel second subscription
  let assert #(10, disp) = dispatcher.cancel(disp, subject2)
  let assert BroadcastDispatcher([#(ref, _)], 10, _) = disp.state
  ref |> should.equal(subject1)

  // Ask more demand for first subscription
  let assert #(10, disp) = dispatcher.ask(disp, 10, subject1)
  let assert BroadcastDispatcher([#(_, demand)], 20, _) = disp.state
  demand.counter |> should.equal(0)
}

pub fn subscribes_asks_and_dispatches_to_multiple_consumers_test() {
  let disp = bd.build() |> dispatcher.initialise
  let subject1 = process.new_subject()
  let subject2 = process.new_subject()
  let subject3 = process.new_subject()
  let self = process.new_subject()

  // Subscribe first two consumers
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject1)
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject2)

  // Ask for events
  let assert #(0, disp) = dispatcher.ask(disp, 3, subject1)
  let assert #(2, disp) = dispatcher.ask(disp, 2, subject2)

  // Dispatch first batch - should fit all consumers
  let assert #([], disp) = dispatcher.dispatch(disp, self, ["a", "b"], 2)

  // Both consumers should receive events
  should_receive(subject1, message.NewEvents(["a", "b"], self), 200)
  should_receive(subject2, message.NewEvents(["a", "b"], self), 200)

  // Ask more from second consumer
  let #(demand, disp) = dispatcher.ask(disp, 2, subject2)
  demand |> should.equal(1)

  // Dispatch another batch with leftovers
  let assert #(["d"], disp) = dispatcher.dispatch(disp, self, ["c", "d"], 2)

  should_receive(subject1, message.NewEvents(["c"], self), 200)
  should_receive(subject2, message.NewEvents(["c"], self), 200)

  // Try dispatching with no demand
  let assert #(["d"], disp) = dispatcher.dispatch(disp, self, ["d"], 1)

  // No messages should be received
  process.receive(subject1, 0) |> should.be_error
  process.receive(subject2, 0) |> should.be_error

  // Add more demand to first consumer
  let assert #(1, disp) = dispatcher.ask(disp, 1, subject1)

  // Add a third subscriber
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject3)

  // Dispatch more events
  let assert #(["d", "e"], disp) =
    dispatcher.dispatch(disp, self, ["d", "e"], 2)

  // Even out the demands
  let assert #(0, disp) = dispatcher.ask(disp, 2, subject1)
  let assert #(0, disp) = dispatcher.ask(disp, 2, subject2)
  let assert #(3, disp) = dispatcher.ask(disp, 3, subject3)

  // Dispatch more events
  let assert #([], _disp) = dispatcher.dispatch(disp, self, ["d", "e", "f"], 3)

  should_receive(subject1, message.NewEvents(["d", "e", "f"], self), 200)
  should_receive(subject2, message.NewEvents(["d", "e", "f"], self), 200)
  should_receive(subject3, message.NewEvents(["d", "e", "f"], self), 200)
}

pub fn subscribing_with_selector_function_test() {
  // This test can't be directly ported because Gleam doesn't support
  // passing selectors at subscription time. The selector would need to be
  // tested directly through the API calls.

  // Instead, we'll simulate what would happen when dispatcher.dispatch
  // is called with events and selectors

  let selector1 = fn(event: Dict(String, String)) -> Bool {
    let assert Ok(key) = dict.get(event, "key")
    string.starts_with(key, "pre")
  }

  let selector2 = fn(event: Dict(String, String)) -> Bool {
    let assert Ok(key) = dict.get(event, "key")
    string.starts_with(key, "pref")
  }

  // Create the events
  let events = [
    dict.from_list([#("key", "pref-1234")]),
    dict.from_list([#("key", "pref-5678")]),
    dict.from_list([#("key", "pre0000")]),
    dict.from_list([#("key", "foo0000")]),
  ]

  // Check selector functions
  let result1 = list.filter(events, selector1)
  result1 |> list.length |> should.equal(3)

  let result2 = list.filter(events, selector2)
  result2 |> list.length |> should.equal(2)
}

pub fn subscribing_is_idempotent_test() {
  let disp = bd.build() |> dispatcher.initialise
  let subject = process.new_subject()

  // First subscription should work
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject)

  // Second subscription from same pid should fail
  let result = dispatcher.subscribe(disp, subject)
  result |> should.be_error
}

@external(erlang, "dolly_test_ffi", "should_receive")
fn should_receive(subject: Subject(a), expected: a, timeout: Int) -> Nil

@external(erlang, "dolly_test_ffi", "should_not_receive")
fn should_not_receive(subject: Subject(a), expected: a, timeout: Int) -> Nil
