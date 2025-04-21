import dolly/dispatcher.{type Behavior, type From, Behavior}
import dolly/internal/message
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set.{type Set}
import logging
import redux/erlang/process.{type Subject}

/// A type to represent a demand with selector function
pub type DemandWithSelector(event) {
  DemandWithSelector(
    counter: Int,
    ref: From(event),
    selector: Option(fn(event) -> Bool),
  )
}

/// The state of the BroadcastDispatcher
pub type BroadcastDispatcher(event) {
  BroadcastDispatcher(
    demands: List(DemandWithSelector(event)),
    waiting: Int,
    subscribed_processes: Set(Subject(message.Consumer(event))),
  )
}

/// The builder for configuring the BroadcastDispatcher
pub opaque type Builder {
  Builder
}

/// Create a new BroadcastDispatcher builder
pub fn new() -> Builder {
  Builder
}

/// Creates initializer function for the dispatcher
pub fn initialiser(
  _builder: Builder,
) -> fn() -> Behavior(BroadcastDispatcher(event), event) {
  fn() { build() }
}

/// Builds the BroadcastDispatcher behavior
pub fn build() -> Behavior(BroadcastDispatcher(event), event) {
  Behavior(initialise: initialise(), ask:, cancel:, dispatch:, subscribe:)
}

/// Initializes the dispatcher state
fn initialise() -> fn() -> BroadcastDispatcher(event) {
  fn() { BroadcastDispatcher([], 0, set.new()) }
}

/// Handles demand requests from consumers
fn ask(
  state: BroadcastDispatcher(event),
  counter: Int,
  from: From(event),
) -> #(Int, BroadcastDispatcher(event)) {
  let #(current, selector, demands) = pop_demand(from, state.demands)
  let demands = add_demand(current + counter, from, selector, demands)
  let new_min = get_min(demands)
  let demands = adjust_demand(new_min, demands)

  #(
    new_min,
    BroadcastDispatcher(
      demands: demands,
      waiting: state.waiting + new_min,
      subscribed_processes: state.subscribed_processes,
    ),
  )
}

/// Handles cancellation of subscriptions
fn cancel(
  state: BroadcastDispatcher(event),
  from: From(event),
) -> #(Int, BroadcastDispatcher(event)) {
  let subscribed_processes = set.delete(state.subscribed_processes, from)

  let demands = delete_demand(from, state.demands)

  case demands {
    [] -> #(0, BroadcastDispatcher([], 0, subscribed_processes))
    _ -> {
      // Since we may have removed the process we were waiting on,
      // cancellation may actually generate demand!
      let new_min = get_min(demands)
      let demands = adjust_demand(new_min, demands)
      #(
        new_min,
        BroadcastDispatcher(
          demands: demands,
          waiting: state.waiting + new_min,
          subscribed_processes: subscribed_processes,
        ),
      )
    }
  }
}

/// Dispatches events to all consumers
fn dispatch(
  state: BroadcastDispatcher(event),
  self: Subject(message.Producer(event)),
  events: List(event),
  length: Int,
) -> #(List(event), BroadcastDispatcher(event)) {
  case state.waiting {
    0 -> #(events, state)
    _ -> {
      let #(deliver_now, deliver_later, waiting) =
        split_events(events, length, state.waiting)

      list.each(state.demands, fn(demand) {
        let #(selected, discarded) =
          filter_and_count(deliver_now, demand.selector)

        // Request more if events were filtered out
        case discarded {
          0 -> Nil
          _ -> {
            process.send(self, message.Ask(discarded, demand.ref))
          }
        }

        // Send the selected events
        process.send(demand.ref, message.NewEvents(selected, self))
      })

      #(deliver_later, BroadcastDispatcher(..state, waiting: waiting))
    }
  }
}

/// Handles new subscriptions
fn subscribe(
  state: BroadcastDispatcher(event),
  from: From(event),
) -> Result(#(Int, BroadcastDispatcher(event)), Nil) {
  case set.contains(state.subscribed_processes, from) {
    True -> {
      logging.log(
        logging.Error,
        "Consumer already registered with this producer. This subscription has been discarded.",
      )
      Error(Nil)
    }
    False -> {
      let subscribed_processes = set.insert(state.subscribed_processes, from)
      let demands = adjust_demand(-state.waiting, state.demands)
      let selector = None

      Ok(#(
        0,
        BroadcastDispatcher(
          demands: add_demand(0, from, selector, demands),
          waiting: 0,
          subscribed_processes: subscribed_processes,
        ),
      ))
    }
  }
}

// Helper function to get minimum demand across all consumers
fn get_min(demands: List(DemandWithSelector(event))) -> Int {
  case demands {
    [] -> 0
    [first, ..rest] -> {
      let min_val =
        list.fold(rest, first.counter, fn(min_so_far, demand) {
          int.min(min_so_far, demand.counter)
        })
      int.max(min_val, 0)
    }
  }
}

// Helper function to split events according to waiting demand
fn split_events(
  events: List(event),
  length: Int,
  counter: Int,
) -> #(List(event), List(event), Int) {
  case length <= counter {
    True -> #(events, [], counter - length)
    False -> {
      let #(now, later) = list.split(events, counter)
      #(now, later, 0)
    }
  }
}

// Helper function to adjust demand by minimum amount
fn adjust_demand(
  min: Int,
  demands: List(DemandWithSelector(event)),
) -> List(DemandWithSelector(event)) {
  case min {
    0 -> demands
    _ ->
      list.map(demands, fn(demand) {
        DemandWithSelector(..demand, counter: demand.counter - min)
      })
  }
}

// Helper function to add demand entry
fn add_demand(
  counter: Int,
  ref: From(event),
  selector: Option(fn(event) -> Bool),
  demands: List(DemandWithSelector(event)),
) -> List(DemandWithSelector(event)) {
  let new_demand = DemandWithSelector(counter, ref, selector)
  [new_demand, ..demands]
}

// Helper function to find and remove demand by reference
fn pop_demand(
  ref: From(event),
  demands: List(DemandWithSelector(event)),
) -> #(Int, Option(fn(event) -> Bool), List(DemandWithSelector(event))) {
  case list.find(demands, fn(d) { d.ref == ref }) {
    Error(Nil) -> todo
    Ok(d) -> todo
  }
  let result =
    list.fold_until(demands, #(None, []), fn(acc, demand) {
      case demand.ref == ref {
        True -> list.Stop(#(Some(#(demand.counter, demand.selector)), acc.1))
        False -> list.Continue(#(acc.0, [demand, ..acc.1]))
      }
    })

  case result.0 {
    Some(#(counter, selector)) -> #(counter, selector, result.1)
    None -> #(0, None, demands)
  }
}

// Helper function to delete demand by reference
fn delete_demand(
  ref: From(event),
  demands: List(DemandWithSelector(event)),
) -> List(DemandWithSelector(event)) {
  list.filter(demands, fn(demand) { demand.ref != ref })
}

// Helper function to filter events with selector and count discarded events
fn filter_and_count(
  messages: List(event),
  maybe_selector: Option(fn(event) -> Bool),
) -> #(List(event), Int) {
  case maybe_selector {
    None -> #(messages, 0)
    Some(selector) -> {
      list.fold(messages, #([], 0), fn(acc, message) {
        case selector(message) {
          True -> #([message, ..acc.0], acc.1)
          False -> #(acc.0, acc.1 + 1)
        }
      })
      |> fn(result) { #(list.reverse(result.0), result.1) }
    }
  }
}
