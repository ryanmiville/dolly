import dolly/buffer.{type Buffer, Take}
import dolly/dispatcher.{type Dispatcher}
import dolly/dispatcher/demand_dispatcher.{type DemandDispatcher}
import dolly/internal/message
import gleam/bool
import gleam/dict.{type Dict}
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set.{type Set}
import redux/erlang/process.{
  type Monitor, type Name, type Pid, type Selector, type Subject,
}
import redux/otp/actor.{type Initialised, type StartError}

pub type Producer(event) {
  Producer(subject: Subject(Msg(event)))
}

pub type Produce(state, event) {
  Next(events: List(event), state: state)
  Done
}

pub type Builder(state, dispatcher, event) {
  Builder(
    initialise: fn() -> state,
    initialise_dispatcher: fn() -> dispatcher.Behavior(dispatcher, event),
    timeout: Int,
    handle_demand: fn(state, Int) -> Produce(state, event),
    buffer_strategy: buffer.Keep,
    buffer_capacity: Int,
    name: Option(Name(Msg(event))),
  )
}

pub fn new(state: state) -> Builder(state, DemandDispatcher(event), event) {
  new_with_initialiser(1000, fn() { state })
}

pub fn new_with_initialiser(
  timeout: Int,
  initialise: fn() -> state,
) -> Builder(state, DemandDispatcher(event), event) {
  let initialise_dispatcher =
    demand_dispatcher.new()
    |> demand_dispatcher.initialiser

  Builder(
    initialise:,
    timeout:,
    initialise_dispatcher:,
    handle_demand: fn(_, _) { Done },
    buffer_strategy: buffer.Last,
    buffer_capacity: 10_000,
    name: None,
  )
}

pub fn named(
  builder: Builder(state, dispatcher, event),
  name: Name(Msg(event)),
) -> Builder(state, dispatcher, event) {
  Builder(..builder, name: Some(name))
}

pub fn handle_demand(
  builder: Builder(state, dispatcher, event),
  handle_demand: fn(state, Int) -> Produce(state, event),
) -> Builder(state, dispatcher, event) {
  Builder(..builder, handle_demand: handle_demand)
}

pub fn buffer_strategy(
  builder: Builder(state, dispatcher, event),
  buffer_strategy: buffer.Keep,
) -> Builder(state, dispatcher, event) {
  Builder(..builder, buffer_strategy: buffer_strategy)
}

pub fn buffer_capacity(
  builder: Builder(state, dispatcher, event),
  buffer_capacity: Int,
) -> Builder(state, dispatcher, event) {
  Builder(..builder, buffer_capacity: buffer_capacity)
}

pub fn start(
  builder: Builder(state, dispatcher, event),
) -> Result(Producer(event), StartError) {
  actor.new_with_initialiser(builder.timeout, initialise(_, builder))
  |> actor.on_message(on_message)
  |> name_actor(builder.name)
  |> actor.start
  |> result.map(fn(a) { a.data })
}

type Msg(event) =
  message.Producer(event)

type Self(event) =
  Subject(Msg(event))

type Consumer(event) =
  Subject(message.Consumer(event))

type State(state, dispatcher, event) {
  State(
    state: state,
    self: Subject(Msg(event)),
    selector: Selector(Msg(event)),
    buffer: Buffer(event),
    dispatcher: Dispatcher(dispatcher, event),
    consumers: Set(Consumer(event)),
    monitors: Dict(Pid, #(Monitor, Consumer(event))),
    handle_demand: fn(state, Int) -> Produce(state, event),
  )
}

fn name_actor(builder, name) {
  case name {
    Some(name) -> actor.named(builder, name)
    None -> builder
  }
}

fn initialise(
  self: Self(event),
  builder: Builder(state, dispatcher, event),
) -> Result(
  Initialised(State(state, dispatcher, event), Msg(event), Producer(event)),
  String,
) {
  let selector =
    process.new_selector()
    |> process.selecting(self, function.identity)

  let buffer =
    buffer.new()
    |> buffer.keep(builder.buffer_strategy)
    |> buffer.capacity(builder.buffer_capacity)

  let dispatcher =
    builder.initialise_dispatcher()
    |> dispatcher.init

  let state =
    State(
      state: builder.initialise(),
      self:,
      selector:,
      buffer:,
      dispatcher:,
      consumers: set.new(),
      monitors: dict.new(),
      handle_demand: builder.handle_demand,
    )
  actor.initialised(state)
  |> actor.selecting(selector)
  |> actor.returning(Producer(self))
  |> Ok
}

type Next(state, dispatcher, event) =
  actor.Next(State(state, dispatcher, event), Msg(event))

fn on_message(
  state: State(state, dispatcher, event),
  message: Msg(event),
) -> Next(state, dispatcher, event) {
  case message {
    message.Ask(demand:, consumer:) -> on_ask(state, demand, consumer)
    message.ConsumerDown(down) -> on_consumer_down(state, down)
    message.ProducerSubscribe(consumer:, demand:) ->
      on_subscribe(state, consumer, demand)
    message.ProducerUnsubscribe(consumer:) -> on_unsubscribe(state, consumer)
  }
}

fn on_ask(
  state: State(state, dispatcher, event),
  demand: Int,
  consumer: Consumer(event),
) -> Next(state, dispatcher, event) {
  let #(demand, dispatcher) = dispatcher.ask(state.dispatcher, demand, consumer)
  State(..state, dispatcher:)
  |> take_from_buffer_or_handle_demand(demand)
}

fn on_consumer_down(
  state: State(state, dispatcher, event),
  down: process.Down,
) -> Next(state, dispatcher, event) {
  let assert process.ProcessDown(pid:, ..) = down
    as "producer was monitoring a port"

  let state = case dict.get(state.monitors, pid) {
    Ok(#(_, consumer)) -> {
      let consumers = set.delete(state.consumers, consumer)
      let monitors = dict.delete(state.monitors, pid)
      let #(_, dispatcher) = dispatcher.cancel(state.dispatcher, consumer)
      State(..state, consumers:, dispatcher:, monitors:)
    }
    _ -> state
  }

  actor.continue(state)
}

fn on_subscribe(
  state: State(state, dispatcher, event),
  consumer: Consumer(event),
  demand: Int,
) -> Next(state, dispatcher, event) {
  let assert Ok(pid) = process.subject_owner(consumer)
    as "subscribee has no PID"
  let mon = process.monitor(pid)

  let selector =
    state.selector
    |> process.selecting_monitors(message.ConsumerDown)

  process.send(state.self, message.Ask(demand, consumer))

  let assert Ok(#(_, dispatcher)) =
    dispatcher.subscribe(state.dispatcher, consumer)
    as "failed to subscribe dispatcher"
  let monitors = state.monitors |> dict.insert(pid, #(mon, consumer))
  let consumers = set.insert(state.consumers, consumer)
  let state = State(..state, selector:, consumers:, dispatcher:, monitors:)
  actor.continue(state) |> actor.with_selector(selector)
}

fn on_unsubscribe(
  state: State(state, dispatcher, event),
  consumer: Consumer(event),
) -> Next(state, dispatcher, event) {
  let assert Ok(pid) = process.subject_owner(consumer)
    as "subscribee has no PID"

  let consumers = set.delete(state.consumers, consumer)
  let monitors = case dict.get(state.monitors, pid) {
    Ok(#(mon, _)) -> {
      process.demonitor_process(mon)
      dict.delete(state.monitors, pid)
    }
    _ -> state.monitors
  }
  let #(_, dispatcher) = dispatcher.cancel(state.dispatcher, consumer)
  let state = State(..state, consumers:, dispatcher:, monitors:)
  actor.continue(state)
}

fn take_from_buffer_or_handle_demand(
  state: State(state, dispatcher, event),
  demand: Int,
) -> Next(state, dispatcher, event) {
  case take_from_buffer(state, demand) {
    #(0, state) -> {
      actor.continue(state)
    }
    #(demand, state) -> {
      case state.handle_demand(state.state, demand) {
        Next(events, new_state) -> {
          State(..state, state: new_state)
          |> dispatch_events(events, list.length(events))
          |> actor.continue
        }
        Done -> actor.stop()
      }
    }
  }
}

fn take_from_buffer(
  state: State(state, dispatcher, event),
  demand: Int,
) -> #(Int, State(state, dispatcher, event)) {
  let Take(buffer, demand_left, events) = buffer.take(state.buffer, demand)
  use <- bool.guard(events == [], #(demand, state))
  let #(events, dispatcher) =
    dispatcher.dispatch(
      state.dispatcher,
      state.self,
      events,
      demand - demand_left,
    )
  let buffer = buffer.store(buffer, events)
  State(..state, buffer:, dispatcher:)
  |> take_from_buffer(demand_left)
}

fn dispatch_events(
  state: State(state, dispatcher, event),
  events: List(event),
  length: Int,
) -> State(state, dispatcher, event) {
  use <- bool.guard(events == [], state)
  use <- guard_no_consumers(state, events)

  let #(events, dispatcher) =
    dispatcher.dispatch(state.dispatcher, state.self, events, length)
  let buffer = buffer.store(state.buffer, events)
  State(..state, buffer:, dispatcher:)
}

fn guard_no_consumers(
  state: State(s, ds, e),
  events: List(e),
  continue: fn() -> State(s, ds, e),
) -> State(s, ds, e) {
  let when_empty = fn() {
    let buffer = buffer.store(state.buffer, events)
    State(..state, buffer:)
  }
  bool.lazy_guard(set.is_empty(state.consumers), when_empty, continue)
}
