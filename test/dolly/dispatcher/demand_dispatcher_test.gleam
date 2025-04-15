import dolly/dispatcher
import dolly/dispatcher/demand_dispatcher.{
  type DemandDispatcher, DemandDispatcher,
} as dd
import dolly/internal/message
import gleam/option.{None, Some}
import gleeunit/should
import redux/erlang/process.{type Subject}

fn assert_received(subject: Subject(a), expected: a, timeout: Int) {
  process.receive(subject, timeout)
  |> should.equal(Ok(expected))
}

pub fn subscribes_and_cancels_test() {
  let disp = dd.new() |> dd.build |> dispatcher.initialise
  let subject = process.new_subject()
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 0)], 0, None, False))
  let assert #(0, disp) = dispatcher.cancel(disp, subject)
  disp.state
  |> should.equal(DemandDispatcher([], 0, None, False))
}

pub fn subscribes_and_asks_and_cancels_test() {
  let disp = dd.new() |> dd.build |> dispatcher.initialise
  let subject = process.new_subject()

  // Subscribe, ask and cancel and leave some demand
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 0)], 0, None, False))

  let assert #(10, disp) = dispatcher.ask(disp, 10, subject)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 10)], 0, Some(10), False))

  let assert #(0, disp) = dispatcher.cancel(disp, subject)
  disp.state
  |> should.equal(DemandDispatcher([], 10, Some(10), False))

  // Subscribe, ask and cancel and leave the same demand
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 0)], 10, Some(10), False))

  let assert #(0, disp) = dispatcher.ask(disp, 5, subject)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 5)], 5, Some(10), False))

  let assert #(0, disp) = dispatcher.cancel(disp, subject)
  disp.state
  |> should.equal(DemandDispatcher([], 10, Some(10), False))
}

pub fn subscribes_and_asks_and_dispatches_test() {
  let disp = dd.new() |> dd.build |> dispatcher.initialise
  let subject = process.new_subject()
  let self = process.new_subject()
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, subject)

  let assert #(3, disp) = dispatcher.ask(disp, 3, subject)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 3)], 0, Some(3), False))

  let assert #([], disp) = dispatcher.dispatch(disp, self, ["a"], 1)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 2)], 0, Some(3), False))
  assert_received(subject, message.NewEvents(["a"], self), 20)

  let assert #(3, disp) = dispatcher.ask(disp, 3, subject)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 5)], 0, Some(3), False))

  let assert #(["g", "h"], disp) =
    dispatcher.dispatch(disp, self, ["b", "c", "d", "e", "f", "g", "h"], 7)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 0)], 0, Some(3), False))
  assert_received(
    subject,
    message.NewEvents(["b", "c", "d", "e", "f"], self),
    20,
  )

  let assert #(["i", "j"], disp) =
    dispatcher.dispatch(disp, self, ["i", "j"], 2)
  disp.state
  |> should.equal(DemandDispatcher([#(subject, 0)], 0, Some(3), False))

  process.receive(subject, 0) |> should.be_error
}

pub fn subscribes_and_asks_multiple_consumers_test() {
  let disp = dd.new() |> dd.build |> dispatcher.initialise
  let sub1 = process.new_subject()
  let sub2 = process.new_subject()
  let sub3 = process.new_subject()

  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, sub1)
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, sub2)
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, sub3)

  let assert #(4, disp) = dispatcher.ask(disp, 4, sub1)
  let assert #(2, disp) = dispatcher.ask(disp, 2, sub2)
  let assert #(3, disp) = dispatcher.ask(disp, 3, sub3)
  disp.state
  |> should.equal(DemandDispatcher(
    [#(sub1, 4), #(sub3, 3), #(sub2, 2)],
    0,
    Some(4),
    False,
  ))

  let assert #(2, disp) = dispatcher.ask(disp, 2, sub3)
  disp.state
  |> should.equal(DemandDispatcher(
    [#(sub3, 5), #(sub1, 4), #(sub2, 2)],
    0,
    Some(4),
    False,
  ))

  let assert #(4, disp) = dispatcher.ask(disp, 4, sub2)
  disp.state
  |> should.equal(DemandDispatcher(
    [#(sub2, 6), #(sub3, 5), #(sub1, 4)],
    0,
    Some(4),
    False,
  ))
}

pub fn subscribes_and_asks_and_dispatches_multiple_consumers_test() {
  let disp = dd.new() |> dd.build |> dispatcher.initialise
  let sub1 = process.new_subject()
  let sub2 = process.new_subject()
  let self = process.new_subject()

  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, sub1)
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, sub2)

  let assert #(3, disp) = dispatcher.ask(disp, 3, sub1)
  let assert #(2, disp) = dispatcher.ask(disp, 2, sub2)
  disp.state
  |> should.equal(DemandDispatcher([#(sub1, 3), #(sub2, 2)], 0, Some(3), False))

  let assert #([], disp) =
    dispatcher.dispatch(disp, self, ["a", "b", "c", "d", "e"], 5)

  assert_received(sub1, message.NewEvents(["a", "b", "c"], self), 20)
  assert_received(sub2, message.NewEvents(["d", "e"], self), 20)

  let assert #(["a", "b", "c"], disp) =
    dispatcher.dispatch(disp, self, ["a", "b", "c"], 3)

  disp.state
  |> should.equal(DemandDispatcher([#(sub1, 0), #(sub2, 0)], 0, Some(3), False))

  process.receive(sub1, 0) |> should.be_error
  process.receive(sub2, 0) |> should.be_error

  // two batches with left over
  let assert #(3, disp) = dispatcher.ask(disp, 3, sub1)
  let assert #(3, disp) = dispatcher.ask(disp, 3, sub2)
  disp.state
  |> should.equal(DemandDispatcher([#(sub1, 3), #(sub2, 3)], 0, Some(3), False))

  let assert #([], disp) = dispatcher.dispatch(disp, self, ["a", "b"], 2)
  disp.state
  |> should.equal(DemandDispatcher([#(sub2, 3), #(sub1, 1)], 0, Some(3), False))
  assert_received(sub1, message.NewEvents(["a", "b"], self), 20)

  let assert #([], disp) = dispatcher.dispatch(disp, self, ["c", "d"], 2)
  disp.state
  |> should.equal(DemandDispatcher([#(sub1, 1), #(sub2, 1)], 0, Some(3), False))
  assert_received(sub2, message.NewEvents(["c", "d"], self), 20)

  // Eliminate the left-over
  let assert #(["g"], disp) =
    dispatcher.dispatch(disp, self, ["e", "f", "g"], 3)
  disp.state
  |> should.equal(DemandDispatcher([#(sub1, 0), #(sub2, 0)], 0, Some(3), False))
  assert_received(sub1, message.NewEvents(["e"], self), 20)
  assert_received(sub2, message.NewEvents(["f"], self), 20)
}

pub fn subscribes_asks_and_dispatches_to_multiple_consumers_with_shuffled_demands_test() {
  let disp =
    dd.new()
    |> dd.shuffle_initial_demands
    |> dd.build
    |> dispatcher.initialise

  let sub1 = process.new_subject()
  let sub2 = process.new_subject()
  let self = process.new_subject()

  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, sub1)
  let assert Ok(#(0, disp)) = dispatcher.subscribe(disp, sub2)

  let assert #(3, disp) = dispatcher.ask(disp, 3, sub1)
  let assert #(2, disp) = dispatcher.ask(disp, 2, sub2)
  disp.state
  |> should.equal(DemandDispatcher([#(sub1, 3), #(sub2, 2)], 0, Some(3), True))

  // demands should be shuffled after first dispatch
  let assert #([], disp) =
    dispatcher.dispatch(disp, self, ["a", "b", "c", "d", "e"], 5)
  // shuffled flag is reset to false after first dispatch
  let assert DemandDispatcher(
    [#(sub1_actual, 0), #(sub2_actual, 0)],
    0,
    Some(3),
    False,
  ) = disp.state

  case sub1_actual == sub1 {
    True -> {
      sub2_actual |> should.equal(sub2)
      assert_received(sub1, message.NewEvents(["a", "b", "c"], self), 20)
      assert_received(sub2, message.NewEvents(["d", "e"], self), 20)
    }
    False -> {
      sub1_actual |> should.equal(sub2)
      sub2_actual |> should.equal(sub1)
      assert_received(sub2, message.NewEvents(["a", "b"], self), 20)
      assert_received(sub1, message.NewEvents(["c", "d", "e"], self), 20)
    }
  }
}
