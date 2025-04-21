import dolly/internal/batch.{type Batch, type Demand, Batch, Demand}
import dolly/internal/message
import dolly/subscription.{type Subscription, Permanent, Temporary, Transient}
import gleam/dict.{type Dict}
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import redux/erlang/process.{
  type ExitReason, type Name, type Pid, type Selector, type Subject,
}
import redux/otp/actor.{type Initialised, type StartError}

pub type Consumer(event) {
  Consumer(subject: Subject(Msg(event)))
}

pub type Consume(state) {
  Continue(state: state)
  Done(ExitReason)
}

pub opaque type Builder(state, event) {
  Builder(
    initialise: fn() -> state,
    timeout: Int,
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
    initialise: initialise,
    timeout: timeout,
    subscriptions: [],
    handle_events: fn(state, _) { Continue(state) },
    name: None,
  )
}

pub fn handle_events(
  builder: Builder(state, event),
  handle_events: fn(state, List(event)) -> Consume(state),
) -> Builder(state, event) {
  Builder(..builder, handle_events:)
}

pub fn named(
  builder: Builder(state, event),
  name: Name(Msg(event)),
) -> Builder(state, event) {
  Builder(..builder, name: Some(name))
}

pub fn add_subscription(
  builder: Builder(state, event),
  subscription,
) -> Builder(state, event) {
  let subscriptions = [subscription, ..builder.subscriptions]
  Builder(..builder, subscriptions:)
}

type Msg(event) =
  message.Consumer(event)

type Self(event) =
  Subject(Msg(event))

type Producer(event) =
  Subject(message.Producer(event))

type Monitor(event) {
  Monitor(
    mon: process.Monitor,
    subject: Producer(event),
    cancel: message.Cancel,
  )
}

type State(state, event) {
  State(
    state: state,
    self: Self(event),
    selector: Selector(Msg(event)),
    producers: Dict(Producer(event), Demand),
    monitors: Dict(Pid, Monitor(event)),
    handle_events: fn(state, List(event)) -> Consume(state),
  )
}

pub fn start(
  builder: Builder(state, event),
) -> Result(Consumer(event), StartError) {
  actor.new_with_initialiser(builder.timeout, initialise(_, builder))
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
      state: builder.initialise(),
      self:,
      selector:,
      producers: dict.new(),
      monitors: dict.new(),
      handle_events: builder.handle_events,
    )

  let result = init_subscriptions(self, builder.subscriptions)

  use _ <- result.map(result)
  actor.initialised(state)
  |> actor.selecting(selector)
  |> actor.returning(Consumer(self))
}

fn name_actor(builder, name) {
  case name {
    Some(name) -> actor.named(builder, name)
    None -> builder
  }
}

@internal
pub fn init_subscriptions(
  self: Subject(Msg(event)),
  subscriptions: List(Subscription(event)),
) -> Result(Nil, String) {
  list.try_each(subscriptions, subscribe(self, _))
  |> result.replace_error(
    "consumer was not able to subscribe to producer because that process is not alive",
  )
}

fn subscribe(
  self: Subject(Msg(event)),
  subscription: Subscription(event),
) -> Result(Nil, Nil) {
  use _ <- result.map(process.subject_owner(subscription.to.subject))
  process.send(self, subscribe_message(subscription))
}

fn subscribe_message(subscription: Subscription(event)) {
  let min_demand =
    option.unwrap(subscription.min_demand, subscription.max_demand / 2)

  // TODO remove jank
  let cancel = case subscription.cancel {
    Permanent -> message.Permanent
    Temporary -> message.Temporary
    Transient -> message.Transient
  }
  message.ConsumerSubscribe(
    subscription.to.subject,
    cancel,
    min_demand,
    subscription.max_demand,
  )
}

type Next(state, event) =
  actor.Next(State(state, event), Msg(event))

fn on_message(
  state: State(state, event),
  message: Msg(event),
) -> Next(state, event) {
  case message {
    message.NewEvents(events:, from:) -> on_new_events(state, events, from)
    message.ConsumerSubscribe(producer:, cancel:, min_demand:, max_demand:) ->
      on_subscribe(state, producer, cancel, min_demand, max_demand)
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
  cancel: message.Cancel,
  min_demand: Int,
  max_demand: Int,
) -> Next(state, event) {
  let assert Ok(pid) = process.subject_owner(producer)
    as "producer subscriber has no PID"
  let mon = process.monitor(pid)
  let selector =
    state.selector
    |> process.selecting_monitors(message.ProducerDown)

  let monitors =
    state.monitors |> dict.insert(pid, Monitor(mon, producer, cancel))
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

  case dict.get(state.monitors, pid) {
    Ok(_) -> {
      process.send(producer, message.ProducerUnsubscribe(state.self))
      cancel(state, pid, process.Normal)
    }
    _ -> actor.continue(state)
  }
}

fn on_producer_down(
  state: State(state, event),
  down: process.Down,
) -> Next(state, event) {
  let assert process.ProcessDown(pid:, reason:, ..) = down
    as "consumer was monitoring a port"

  cancel(state, pid, reason)
  // let state = case dict.get(state.monitors, pid) {
  //   Ok(Monitor(_, producer, _)) -> {
  //     let producers = dict.delete(state.producers, producer)
  //     let monitors = dict.delete(state.monitors, pid)
  //     State(..state, producers:, monitors:)
  //   }
  //   _ -> state
  // }

  // actor.continue(state)
}

fn cancel(
  state: State(state, event),
  pid: Pid,
  reason: ExitReason,
) -> Next(state, event) {
  case dict.get(state.monitors, pid) {
    Ok(Monitor(mon, producer, cancel)) -> {
      process.demonitor_process(mon)
      let producers = dict.delete(state.producers, producer)
      let monitors = dict.delete(state.monitors, pid)
      let state = State(..state, producers:, monitors:)
      case cancel {
        // todo add reason
        message.Permanent -> actor.stop()
        // todo add reason
        message.Transient if reason != process.Normal -> actor.stop()
        _ -> actor.continue(state)
      }
    }
    _ -> actor.continue(state)
  }
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
