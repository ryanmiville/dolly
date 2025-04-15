import dolly/dispatcher.{type Behavior, type From, Behavior}
import dolly/internal/message
import gleam/bool
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import logging
import redux/erlang/process.{type Subject}

pub type Demand(event) =
  #(From(event), Int)

pub opaque type DemandDispatcher(event) {
  DemandDispatcher(
    demands: List(Demand(event)),
    pending: Int,
    max_demand: Option(Int),
    shuffle: Bool,
  )
}

pub opaque type Builder {
  Builder(max_demand: Option(Int), shuffle: Bool)
}

pub fn new() -> Builder {
  Builder(None, False)
}

pub fn shuffle_initial_demands(builder: Builder) -> Builder {
  Builder(..builder, shuffle: True)
}

pub fn max_demand(builder: Builder, max_demand: Int) -> Builder {
  Builder(..builder, max_demand: Some(max_demand))
}

pub fn initialiser(builder) {
  fn() { Behavior(init: init(builder), ask:, cancel:, dispatch:, subscribe:) }
}

fn init(builder: Builder) -> fn() -> DemandDispatcher(event) {
  fn() { DemandDispatcher([], 0, builder.max_demand, builder.shuffle) }
}

fn ask(
  state: DemandDispatcher(event),
  demand: Int,
  from: From(event),
) -> #(Int, DemandDispatcher(event)) {
  let max = option.unwrap(state.max_demand, demand)

  case demand > max {
    True ->
      logging.log(
        logging.Warning,
        "Dispatcher expects a max demand of "
          <> int.to_string(max)
          <> " but got demand for "
          <> int.to_string(demand)
          <> " events",
      )
    _ -> Nil
  }

  let demands = case list.key_pop(state.demands, from) {
    Error(Nil) -> state.demands
    Ok(#(current, demands)) -> {
      add_demand(demands, from, current + demand)
    }
  }
  let already_sent = int.min(state.pending, demand)
  let state =
    DemandDispatcher(
      ..state,
      demands:,
      pending: state.pending - already_sent,
      max_demand: Some(max),
    )
  #(demand - already_sent, state)
}

fn add_demand(
  demands: List(Demand(event)),
  from: From(event),
  counter: Int,
) -> List(#(From(event), Int)) {
  case demands {
    [] -> [#(from, counter)]
    [#(_, current), ..] if counter > current -> [#(from, counter), ..demands]
    [demand, ..rest] -> [demand, ..add_demand(rest, from, counter)]
  }
}

fn cancel(
  state: DemandDispatcher(event),
  from: From(event),
) -> #(Int, DemandDispatcher(event)) {
  let state = case list.key_pop(state.demands, from) {
    Error(Nil) -> state
    Ok(#(current, demands)) ->
      DemandDispatcher(
        ..state,
        demands: demands,
        pending: current + state.pending,
        max_demand: state.max_demand,
      )
  }
  #(0, state)
}

fn dispatch(
  state: DemandDispatcher(event),
  self: Subject(message.Producer(event)),
  events: List(event),
  length: Int,
) -> #(List(event), DemandDispatcher(event)) {
  use <- guard_shuffle(state, self, events, length)
  let #(events, demands) = dispatch_loop(state.demands, self, events, length)
  #(events, DemandDispatcher(..state, demands:))
}

fn guard_shuffle(
  state: DemandDispatcher(event),
  self: Subject(message.Producer(event)),
  events: List(event),
  length: Int,
  continue: fn() -> #(List(event), DemandDispatcher(event)),
) -> #(List(event), DemandDispatcher(event)) {
  let shuffle = fn() {
    let state = DemandDispatcher(..state, shuffle: False)
    dispatch(state, self, list.shuffle(events), length)
  }
  bool.lazy_guard(state.shuffle, shuffle, continue)
}

fn dispatch_loop(
  demands: List(Demand(event)),
  self: Subject(message.Producer(event)),
  events: List(event),
  length: Int,
) -> #(List(event), List(Demand(event))) {
  use <- bool.guard(events == [], #(events, demands))

  case demands {
    [] | [#(_, 0), ..] -> #(events, demands)
    [#(from, counter), ..rest] -> {
      let #(now, later, length, counter) = split_events(events, length, counter)
      process.send(from, message.NewEvents(now, self))
      let demands = add_demand(rest, from, counter)
      dispatch_loop(demands, self, later, length)
    }
  }
}

fn split_events(
  events: List(event),
  length: Int,
  counter: Int,
) -> #(List(event), List(event), Int, Int) {
  case length <= counter {
    True -> #(events, [], 0, counter - length)
    False -> {
      let #(now, later) = list.split(events, counter)
      #(now, later, length - counter, 0)
    }
  }
}

fn subscribe(
  state: DemandDispatcher(event),
  from: From(event),
) -> Result(#(Int, DemandDispatcher(event)), Nil) {
  let state =
    DemandDispatcher(..state, demands: list.append(state.demands, [#(from, 0)]))
  Ok(#(0, state))
}
