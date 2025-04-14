import dolly/internal/message
import gleam/result
import redux/erlang/process.{type Subject}

pub type From(event) =
  Subject(message.Consumer(event))

pub opaque type Dispatcher(state, event) {
  Dispatcher(state: state, behavior: Behavior(state, event))
}

pub type Behavior(state, event) {
  Behavior(
    init: fn() -> state,
    ask: fn(Int, From(event), state) -> #(Int, state),
    cancel: fn(From(event), state) -> #(Int, state),
    dispatch: fn(List(event), Int, state) -> #(List(event), state),
    subscribe: fn(From(event), state) -> Result(#(Int, state), Nil),
  )
}

pub fn init(behavior: Behavior(state, event)) -> Dispatcher(state, event) {
  Dispatcher(state: behavior.init(), behavior:)
}

pub fn ask(
  dispatcher: Dispatcher(state, event),
  demand: Int,
  from: From(event),
) -> #(Int, Dispatcher(state, event)) {
  let #(new_demand, state) =
    dispatcher.behavior.ask(demand, from, dispatcher.state)
  #(new_demand, Dispatcher(..dispatcher, state:))
}

pub fn cancel(
  dispatcher: Dispatcher(state, event),
  from: From(event),
) -> #(Int, Dispatcher(state, event)) {
  let #(new_demand, state) = dispatcher.behavior.cancel(from, dispatcher.state)
  #(new_demand, Dispatcher(..dispatcher, state:))
}

pub fn dispatch(
  dispatcher: Dispatcher(state, event),
  events: List(event),
  length: Int,
) -> #(List(event), Dispatcher(state, event)) {
  let #(events, state) =
    dispatcher.behavior.dispatch(events, length, dispatcher.state)
  #(events, Dispatcher(..dispatcher, state:))
}

pub fn subscribe(
  dispatcher: Dispatcher(state, event),
  from: From(event),
) -> Result(#(Int, Dispatcher(state, event)), Nil) {
  let sub = dispatcher.behavior.subscribe(from, dispatcher.state)
  use #(demand, state) <- result.map(sub)
  #(demand, Dispatcher(..dispatcher, state:))
}
