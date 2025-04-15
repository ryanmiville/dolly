import dolly/internal/message
import gleam/result
import redux/erlang/process.{type Subject}

pub type From(event) =
  Subject(message.Consumer(event))

pub type Dispatcher(state, event) {
  Dispatcher(state: state, behavior: Behavior(state, event))
}

pub type Behavior(state, event) {
  Behavior(
    initialise: fn() -> state,
    ask: fn(state, Int, From(event)) -> #(Int, state),
    cancel: fn(state, From(event)) -> #(Int, state),
    dispatch: fn(state, Subject(message.Producer(event)), List(event), Int) ->
      #(List(event), state),
    subscribe: fn(state, From(event)) -> Result(#(Int, state), Nil),
  )
}

pub fn initialise(behavior: Behavior(state, event)) -> Dispatcher(state, event) {
  Dispatcher(state: behavior.initialise(), behavior:)
}

pub fn ask(
  dispatcher: Dispatcher(state, event),
  demand: Int,
  from: From(event),
) -> #(Int, Dispatcher(state, event)) {
  let #(new_demand, state) =
    dispatcher.behavior.ask(dispatcher.state, demand, from)
  #(new_demand, Dispatcher(..dispatcher, state:))
}

pub fn cancel(
  dispatcher: Dispatcher(state, event),
  from: From(event),
) -> #(Int, Dispatcher(state, event)) {
  let #(new_demand, state) = dispatcher.behavior.cancel(dispatcher.state, from)
  #(new_demand, Dispatcher(..dispatcher, state:))
}

pub fn dispatch(
  dispatcher: Dispatcher(state, event),
  self: Subject(message.Producer(event)),
  events: List(event),
  length: Int,
) -> #(List(event), Dispatcher(state, event)) {
  let #(events, state) =
    dispatcher.behavior.dispatch(dispatcher.state, self, events, length)
  #(events, Dispatcher(..dispatcher, state:))
}

pub fn subscribe(
  dispatcher: Dispatcher(state, event),
  from: From(event),
) -> Result(#(Int, Dispatcher(state, event)), Nil) {
  let sub = dispatcher.behavior.subscribe(dispatcher.state, from)
  use #(demand, state) <- result.map(sub)
  #(demand, Dispatcher(..dispatcher, state:))
}
