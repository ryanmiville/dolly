import dolly/internal/batch.{type Batch, type Demand, Batch, Demand}
import dolly/internal/message
import dolly/subscription.{type Subscription}
import gleam/dict.{type Dict}
import gleam/function
import gleam/option.{type Option, None, Some}
import gleam/result
import redux/erlang/process.{
  type ExitReason, type Monitor, type Name, type Pid, type Selector,
  type Subject,
}
import redux/otp/actor.{type Initialised, type StartError}

pub type Consumer(event) {
  Consumer(subject: Subject(Msg(event)))
}

pub type Consume(state) {
  Continue(state: state)
  Done(ExitReason)
}

pub type Builder(state, event) {
  Builder(
    init: fn() -> state,
    init_timeout: Int,
    subscriptions: List(Subscription(event)),
    handle_events: fn(state, List(event)) -> Consume(state),
    name: Option(Name(Msg(event))),
  )
}

pub fn new(state: state) -> Builder(state, event) {
  new_with_initialiser(1000, fn() { state })
}

pub fn new_with_initialiser(
  timeout: Int,
  initialise: fn() -> state,
) -> Builder(state, event) {
  Builder(
    init: initialise,
    init_timeout: timeout,
    subscriptions: [],
    handle_events: fn(state, _) { Continue(state) },
    name: None,
  )
}

type Msg(event) =
  message.Consumer(event)

type Self(event) =
  Subject(Msg(event))

type Producer(event) =
  Subject(message.Producer(event))

type State(state, event) {
  State(
    state: state,
    self: Self(event),
    selector: Selector(Msg(event)),
    producers: Dict(Producer(event), Demand),
    monitors: Dict(Pid, #(Monitor, Producer(event))),
    handle_events: fn(state, List(event)) -> Consume(state),
  )
}

pub fn start(
  builder: Builder(state, event),
) -> Result(Consumer(event), StartError) {
  actor.new_with_initialiser(builder.init_timeout, initialise(_, builder))
  |> actor.on_message(on_message)
  |> name_actor(builder.name)
  |> actor.start
  |> result.map(fn(a) { a.data })
}

fn initialise(
  self: Self(event),
  builder: Builder(state, event),
) -> Result(
  Initialised(State(state, event), Msg(event), Consumer(event)),
  String,
) {
  let selector =
    process.new_selector()
    |> process.selecting(self, function.identity)

  let state =
    State(
      state: builder.init(),
      self:,
      selector:,
      producers: dict.new(),
      monitors: dict.new(),
      handle_events: builder.handle_events,
    )

  actor.initialised(state)
  |> actor.selecting(selector)
  |> actor.returning(Consumer(self))
  |> Ok
}

fn name_actor(builder, name) {
  case name {
    Some(name) -> actor.named(builder, name)
    None -> builder
  }
}

type Next(state, event) =
  actor.Next(State(state, event), Msg(event))

fn on_message(
  state: State(state, event),
  message: Msg(event),
) -> Next(state, event) {
  case message {
    message.NewEvents(events:, from:) -> on_new_events(state, events, from)
    message.ConsumerSubscribe(producer:, min_demand:, max_demand:) ->
      on_subscribe(state, producer, min_demand, max_demand)
    message.ConsumerUnsubscribe(producer:) -> on_unsubscribe(state, producer)
    message.ProducerDown(down) -> on_producer_down(state, down)
  }
}

fn on_new_events(
  state: State(state, event),
  events: List(event),
  from: Producer(event),
) -> Next(state, event) {
  case dict.get(state.producers, from) {
    Ok(demand) -> {
      let #(current, batches) = batch.events(events, demand)
      let demand = Demand(..demand, current:)
      let producers = dict.insert(state.producers, from, demand)
      let state = State(..state, producers:)
      dispatch(state, batches, from)
    }
    Error(_) -> actor.continue(state)
  }
}

fn on_subscribe(
  state: State(state, event),
  producer: Producer(event),
  min_demand: Int,
  max_demand: Int,
) -> Next(state, event) {
  let assert Ok(pid) = process.subject_owner(producer)
    as "producer subscriber has no PID"
  let mon = process.monitor(pid)
  let selector =
    state.selector
    |> process.selecting_monitors(message.ProducerDown)

  let monitors = state.monitors |> dict.insert(pid, #(mon, producer))
  let producers =
    dict.insert(
      state.producers,
      producer,
      Demand(current: max_demand, min: min_demand, max: max_demand),
    )
  let state = State(..state, selector:, producers:, monitors:)
  process.send(producer, message.ProducerSubscribe(state.self, max_demand))
  actor.continue(state) |> actor.with_selector(selector)
}

fn on_unsubscribe(
  state: State(state, event),
  producer: Producer(event),
) -> Next(state, event) {
  let assert Ok(pid) = process.subject_owner(producer)
    as "producer subscriber has no PID"

  let producers = dict.delete(state.producers, producer)
  let monitors = case dict.get(state.monitors, pid) {
    Ok(#(mon, _)) -> {
      process.demonitor_process(mon)
      dict.delete(state.monitors, pid)
    }
    _ -> state.monitors
  }
  process.send(producer, message.ProducerUnsubscribe(state.self))
  State(..state, producers:, monitors:)
  |> actor.continue
}

fn on_producer_down(
  state: State(state, event),
  down: process.Down,
) -> Next(state, event) {
  let assert process.ProcessDown(pid:, ..) = down
    as "consumer was monitoring a port"

  let state = case dict.get(state.monitors, pid) {
    Ok(#(_, producer)) -> {
      let producers = dict.delete(state.producers, producer)
      let monitors = dict.delete(state.monitors, pid)
      State(..state, producers:, monitors:)
    }
    _ -> state
  }

  actor.continue(state)
}

fn dispatch(
  state: State(state, event),
  batches: List(Batch(event)),
  from: Producer(event),
) -> Next(state, event) {
  case batches {
    [] -> actor.continue(state)
    [Batch(events:, size:), ..rest] -> {
      case state.handle_events(state.state, events) {
        Continue(new_state) -> {
          process.send(from, message.Ask(size, state.self))
          State(..state, state: new_state)
          |> dispatch(rest, from)
        }
        Done(_) -> actor.stop()
      }
    }
  }
}
