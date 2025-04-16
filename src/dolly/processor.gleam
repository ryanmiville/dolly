import dolly/buffer.{type Buffer, Take}
import dolly/consumer.{type Consumer}
import dolly/dispatcher.{type Dispatcher}
import dolly/dispatcher/demand_dispatcher.{type DemandDispatcher}
import dolly/internal/batch.{type Batch, type Demand, Batch, Demand}
import dolly/internal/message
import dolly/producer.{type Produce, type Producer}
import dolly/subscription.{type Subscription}
import gleam/bool
import gleam/deque.{type Deque}
import gleam/dict.{type Dict}
import gleam/function
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set.{type Set}
import redux/erlang/process.{
  type Monitor, type Name, type Pid, type Selector, type Subject,
}
import redux/otp/actor.{type Initialised, type StartError}

pub type Processor(in, out) {
  Processor(
    subject: Subject(message.Processor(in, out)),
    consumer_subject: Subject(message.Consumer(in)),
    producer_subject: Subject(message.Producer(out)),
  )
}

pub type Builder(state, dispatcher, in, out) {
  Builder(
    initialise: fn() -> state,
    initialise_dispatcher: fn() -> dispatcher.Behavior(dispatcher, out),
    timeout: Int,
    handle_events: fn(state, List(in)) -> Produce(state, out),
    buffer_strategy: buffer.Keep,
    buffer_capacity: Option(Int),
    subscriptions: List(Subscription(in)),
    name: Option(Name(message.Producer(out))),
  )
}

pub fn new(state: state) -> Builder(state, DemandDispatcher(out), in, out) {
  new_with_initialiser(1000, fn() { state })
}

pub fn new_with_initialiser(
  timeout: Int,
  initialise: fn() -> state,
) -> Builder(state, DemandDispatcher(out), in, out) {
  let initialise_dispatcher =
    demand_dispatcher.new()
    |> demand_dispatcher.initialiser

  Builder(
    initialise:,
    timeout:,
    initialise_dispatcher:,
    handle_events: fn(_, _) { producer.Done },
    buffer_strategy: buffer.Last,
    // TODO make option for infinity
    buffer_capacity: None,
    subscriptions: [],
    name: None,
  )
}

pub fn named(
  builder: Builder(state, dispatcher, in, out),
  name: Name(message.Producer(out)),
) -> Builder(state, dispatcher, in, out) {
  Builder(..builder, name: Some(name))
}

pub fn handle_events(
  builder: Builder(state, dispatcher, in, out),
  handle_events: fn(state, List(in)) -> Produce(state, out),
) -> Builder(state, dispatcher, in, out) {
  Builder(..builder, handle_events:)
}

pub fn buffer_strategy(
  builder: Builder(state, dispatcher, in, out),
  buffer_strategy: buffer.Keep,
) -> Builder(state, dispatcher, in, out) {
  Builder(..builder, buffer_strategy:)
}

pub fn buffer_capacity(
  builder: Builder(state, dispatcher, in, out),
  buffer_capacity: Int,
) -> Builder(state, dispatcher, in, out) {
  Builder(..builder, buffer_capacity: Some(buffer_capacity))
}

pub fn add_subscription(
  builder: Builder(state, dispatcher, in, out),
  subscription: Subscription(in),
) -> Builder(state, dispatcher, in, out) {
  let subscriptions = [subscription, ..builder.subscriptions]
  Builder(..builder, subscriptions:)
}

pub fn as_producer(processor: Processor(in, out)) -> Producer(out) {
  producer.Producer(processor.producer_subject)
}

pub fn as_consumer(processor: Processor(in, out)) -> Consumer(in) {
  consumer.Consumer(processor.consumer_subject)
}

pub fn start(
  builder: Builder(state, dispatcher, in, out),
) -> Result(Processor(in, out), StartError) {
  actor.new_with_initialiser(builder.timeout, initialise(_, builder))
  |> actor.on_message(on_message)
  |> actor.start
  |> result.map(fn(a) { a.data })
}

fn try_register_self(name: Name(msg)) -> Result(Nil, String) {
  case process.register(process.self(), name) {
    Ok(Nil) -> Ok(Nil)
    Error(_) -> Error("name already registered")
  }
}

fn initialise(
  self: Self(in, out),
  builder: Builder(state, dispatcher, in, out),
) -> Result(
  Initialised(
    State(state, dispatcher, in, out),
    Msg(in, out),
    Processor(in, out),
  ),
  String,
) {
  let self_producer = case builder.name {
    None -> {
      Ok(process.new_subject())
    }
    Some(name) -> {
      use _ <- result.map(try_register_self(name))
      process.named_subject(name)
    }
  }
  use self_producer <- result.try(self_producer)
  let self_consumer = process.new_subject()

  let selector =
    process.new_selector()
    |> process.selecting(self, function.identity)
    |> process.selecting(self_consumer, message.ConsumerMsg)
    |> process.selecting(self_producer, message.ProducerMsg)

  let buffer = case builder.buffer_capacity {
    Some(cap) -> {
      buffer.new()
      |> buffer.keep(builder.buffer_strategy)
      |> buffer.capacity(cap)
    }
    None -> {
      buffer.new()
      |> buffer.keep(builder.buffer_strategy)
    }
  }

  let dispatcher = builder.initialise_dispatcher() |> dispatcher.initialise

  let state =
    State(
      state: builder.initialise(),
      self:,
      self_producer:,
      self_consumer:,
      selector:,
      buffer:,
      dispatcher:,
      producers: dict.new(),
      producer_monitors: dict.new(),
      consumers: set.new(),
      consumer_monitors: dict.new(),
      events: Events(queue: deque.new(), demand: 0),
      handle_events: builder.handle_events,
    )

  let result = consumer.init_subscriptions(self_consumer, builder.subscriptions)

  use _ <- result.map(result)
  actor.initialised(state)
  |> actor.selecting(selector)
  |> actor.returning(Processor(self, self_consumer, self_producer))
}

type Msg(in, out) =
  message.Processor(in, out)

type Self(in, out) =
  Subject(message.Processor(in, out))

type SubConsumer(event) =
  Subject(message.Consumer(event))

type SubProducer(event) =
  Subject(message.Producer(event))

type Events(in) {
  Events(queue: Deque(#(List(in), SubProducer(in))), demand: Int)
}

type State(state, dispatcher, in, out) {
  State(
    state: state,
    self: Self(in, out),
    self_producer: SubProducer(out),
    self_consumer: SubConsumer(in),
    selector: Selector(Msg(in, out)),
    buffer: Buffer(out),
    dispatcher: Dispatcher(dispatcher, out),
    producers: Dict(SubProducer(in), Demand),
    producer_monitors: Dict(Pid, #(Monitor, SubProducer(in))),
    consumers: Set(SubConsumer(out)),
    consumer_monitors: Dict(Pid, #(Monitor, SubConsumer(out))),
    events: Events(in),
    handle_events: fn(state, List(in)) -> Produce(state, out),
  )
}

type Next(state, dispatcher, in, out) =
  actor.Next(State(state, dispatcher, in, out), Msg(in, out))

fn on_message(
  state: State(state, dispatcher, in, out),
  message: Msg(in, out),
) -> Next(state, dispatcher, in, out) {
  case message {
    message.ProducerMsg(msg) -> on_producer_message(state, msg)
    message.ConsumerMsg(msg) -> on_consumer_message(state, msg)
  }
}

fn on_producer_message(
  state: State(state, dispatcher, in, out),
  message: message.Producer(out),
) -> Next(state, dispatcher, in, out) {
  case message {
    message.Ask(demand:, consumer:) -> on_ask(state, demand, consumer)
    message.ConsumerDown(down) -> on_consumer_down(state, down)
    message.ProducerSubscribe(consumer:, demand:) ->
      on_producer_subscribe(state, consumer, demand)
    message.ProducerUnsubscribe(consumer:) ->
      on_producer_unsubscribe(state, consumer)
    message.Accumulate -> actor.continue(state)
    message.Forward -> actor.continue(state)
  }
}

fn on_producer_unsubscribe(
  state: State(state, dispatcher, in, out),
  consumer: SubConsumer(out),
) -> actor.Next(State(state, dispatcher, in, out), Msg(in, out)) {
  let assert Ok(pid) = process.subject_owner(consumer)
    as "subscribee has no PID"

  let consumers = set.delete(state.consumers, consumer)
  let consumer_monitors = case dict.get(state.consumer_monitors, pid) {
    Ok(#(mon, _)) -> {
      process.demonitor_process(mon)
      dict.delete(state.consumer_monitors, pid)
    }
    _ -> state.consumer_monitors
  }
  let #(_, dispatcher) = dispatcher.cancel(state.dispatcher, consumer)
  let state = State(..state, consumers:, dispatcher:, consumer_monitors:)
  actor.continue(state)
}

fn on_producer_subscribe(
  state: State(state, dispatcher, in, out),
  consumer: SubConsumer(out),
  demand: Int,
) -> actor.Next(State(state, dispatcher, in, out), Msg(in, out)) {
  let assert Ok(pid) = process.subject_owner(consumer)
    as "subscribee has no PID"
  let mon = process.monitor(pid)

  let selector =
    state.selector
    |> process.selecting_monitors(fn(down) {
      message.ProducerMsg(message.ConsumerDown(down))
    })

  process.send(state.self_producer, message.Ask(demand, consumer))

  let assert Ok(#(_, dispatcher)) =
    dispatcher.subscribe(state.dispatcher, consumer)
    as "failed to subscribe dispatcher"
  let consumer_monitors =
    state.consumer_monitors |> dict.insert(pid, #(mon, consumer))
  let consumers = set.insert(state.consumers, consumer)
  let state =
    State(..state, selector:, consumers:, dispatcher:, consumer_monitors:)
  actor.continue(state) |> actor.with_selector(selector)
}

fn on_consumer_down(
  state: State(state, dispatcher, in, out),
  down: process.Down,
) -> actor.Next(State(state, dispatcher, in, out), Msg(in, out)) {
  let assert process.ProcessDown(pid:, ..) = down
    as "producer was monitoring a port"

  let state = case dict.get(state.consumer_monitors, pid) {
    Ok(#(_, consumer)) -> {
      let consumers = set.delete(state.consumers, consumer)
      let consumer_monitors = dict.delete(state.consumer_monitors, pid)
      let #(_, dispatcher) = dispatcher.cancel(state.dispatcher, consumer)
      State(..state, consumers:, dispatcher:, consumer_monitors:)
    }
    _ -> state
  }

  actor.continue(state)
}

fn on_ask(
  state: State(state, dispatcher, in, out),
  demand: Int,
  consumer: SubConsumer(out),
) -> actor.Next(State(state, dispatcher, in, out), Msg(in, out)) {
  let #(counter, dispatcher) =
    dispatcher.ask(state.dispatcher, demand, consumer)

  let Events(queue, demand) = state.events
  let counter = counter + demand

  let #(_, state) =
    State(..state, dispatcher:, events: Events(queue, counter))
    |> take_from_buffer(counter)
  let Events(queue, demand) = state.events
  take_events(state, queue, demand)
}

fn take_from_buffer(
  state: State(state, dispatcher, in, out),
  demand: Int,
) -> #(Int, State(state, dispatcher, in, out)) {
  let Take(buffer, demand_left, events) = buffer.take(state.buffer, demand)
  use <- bool.guard(events == [], #(demand, state))

  State(..state, buffer:)
  |> dispatch_events(events, demand - demand_left)
  |> take_from_buffer(demand_left)
}

fn dispatch_events(
  state: State(state, dispatcher, in, out),
  events: List(out),
  length: Int,
) -> State(state, dispatcher, in, out) {
  use <- bool.guard(events == [], state)
  use <- guard_no_consumers(state, events)

  let #(events, dispatcher) =
    dispatcher.dispatch(state.dispatcher, state.self_producer, events, length)

  let Events(queue, demand) = state.events
  let demand = demand - { length - list.length(events) }

  let buffer = buffer.store(state.buffer, events)
  let events = Events(queue, int.max(demand, 0))
  State(..state, buffer:, dispatcher:, events:)
}

fn take_events(
  state: State(state, dispatcher, in, out),
  queue: Deque(#(List(in), SubProducer(in))),
  demand: Int,
) -> Next(state, dispatcher, in, out) {
  use <- guard_no_demand(state, queue, demand)
  case deque.pop_front(queue) {
    Ok(#(#(events, from), queue)) -> {
      let state = State(..state, events: Events(queue, demand))
      case send_events(state, events, from) {
        producer.Next(_, state) ->
          take_events(state, state.events.queue, state.events.demand)
        producer.Done -> actor.stop()
      }
    }
    _ -> {
      State(..state, events: Events(queue, demand))
      |> actor.continue
    }
  }
}

fn send_events(
  state: State(state, dispatcher, in, out),
  events: List(in),
  from: SubProducer(in),
) -> Produce(State(state, dispatcher, in, out), a) {
  case dict.get(state.producers, from) {
    Ok(demand) -> {
      let #(current, batches) = batch.events(events, demand)
      let demand = Demand(..demand, current:)
      let producers = dict.insert(state.producers, from, demand)
      State(..state, producers:) |> dispatch(batches, from)
    }
    _ -> {
      // We queued but producer was removed
      let batches = [Batch(events, 0)]
      dispatch(state, batches, from)
    }
  }
}

fn dispatch(
  state: State(state, dispatcher, in, out),
  batches: List(Batch(in)),
  from: SubProducer(in),
) -> Produce(State(state, dispatcher, in, out), a) {
  case batches {
    [] -> producer.Next([], state)
    [Batch(events:, size:), ..rest] -> {
      case state.handle_events(state.state, events) {
        producer.Next(events, new_state) -> {
          let state =
            State(..state, state: new_state)
            |> dispatch_events(events, list.length(events))
          process.send(from, message.Ask(size, state.self_consumer))
          dispatch(state, rest, from)
        }
        producer.Done -> producer.Done
      }
    }
  }
}

fn guard_no_consumers(
  state: State(s, ds, i, o),
  events: List(o),
  continue: fn() -> State(s, ds, i, o),
) -> State(s, ds, i, o) {
  let when_empty = fn() {
    let buffer = buffer.store(state.buffer, events)
    State(..state, buffer:)
  }
  bool.lazy_guard(set.is_empty(state.consumers), when_empty, continue)
}

fn guard_no_demand(state, queue, demand, continue) {
  let when_no_demand = fn() {
    State(..state, events: Events(queue, demand)) |> actor.continue
  }
  bool.lazy_guard(demand <= 0, when_no_demand, continue)
}

fn on_consumer_message(
  state: State(state, dispatcher, in, out),
  message: message.Consumer(in),
) -> Next(state, dispatcher, in, out) {
  case message {
    message.NewEvents(events:, from:) -> on_new_events(state, events, from)
    message.ConsumerSubscribe(producer:, min_demand:, max_demand:) ->
      on_consumer_subscribe(state, producer, min_demand, max_demand)
    message.ConsumerUnsubscribe(producer:) ->
      on_consumer_unsubscribe(state, producer)
    message.ProducerDown(down) -> on_producer_down(state, down)
  }
}

fn on_producer_down(
  state: State(state, dispatcher, in, out),
  down: process.Down,
) -> actor.Next(State(state, dispatcher, in, out), Msg(in, out)) {
  let assert process.ProcessDown(pid:, ..) = down
    as "consumer was monitoring a port"

  let state = case dict.get(state.producer_monitors, pid) {
    Ok(#(_, producer)) -> {
      let producers = dict.delete(state.producers, producer)
      let producer_monitors = dict.delete(state.producer_monitors, pid)
      State(..state, producers:, producer_monitors:)
    }
    _ -> state
  }

  actor.continue(state)
}

fn on_consumer_unsubscribe(
  state: State(state, dispatcher, in, out),
  producer: SubProducer(in),
) -> actor.Next(State(state, dispatcher, in, out), Msg(in, out)) {
  let assert Ok(pid) = process.subject_owner(producer)
    as "producer subscriber has no PID"

  let producers = dict.delete(state.producers, producer)
  let producer_monitors = case dict.get(state.producer_monitors, pid) {
    Ok(#(mon, _)) -> {
      process.demonitor_process(mon)
      dict.delete(state.producer_monitors, pid)
    }
    _ -> state.producer_monitors
  }
  process.send(producer, message.ProducerUnsubscribe(state.self_consumer))
  State(..state, producers:, producer_monitors:)
  |> actor.continue
}

fn on_consumer_subscribe(
  state: State(state, dispatcher, in, out),
  producer: SubProducer(in),
  min_demand: Int,
  max_demand: Int,
) -> actor.Next(State(state, dispatcher, in, out), Msg(in, out)) {
  let assert Ok(pid) = process.subject_owner(producer)
    as "producer subscriber has no PID"
  let mon = process.monitor(pid)
  let selector =
    state.selector
    |> process.selecting_monitors(fn(down) {
      message.ConsumerMsg(message.ProducerDown(down))
    })

  let producer_monitors =
    state.producer_monitors |> dict.insert(pid, #(mon, producer))
  let producers =
    dict.insert(
      state.producers,
      producer,
      Demand(current: max_demand, min: min_demand, max: max_demand),
    )
  let state = State(..state, selector:, producers:, producer_monitors:)
  process.send(
    producer,
    message.ProducerSubscribe(state.self_consumer, max_demand),
  )
  actor.continue(state) |> actor.with_selector(selector)
}

fn on_new_events(
  state: State(state, dispatcher, in, out),
  events: List(in),
  from: SubProducer(in),
) -> actor.Next(State(state, dispatcher, in, out), Msg(in, out)) {
  let queue = deque.push_back(state.events.queue, #(events, from))
  take_events(state, queue, state.events.demand)
}
