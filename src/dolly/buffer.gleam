import gleam/bool
import gleam/deque
import gleam/list
import gleam/option.{type Option, None, Some}

pub type Keep {
  First
  Last
}

pub type Buffer(event) {
  Buffer(
    queue: deque.Deque(event),
    counter: Int,
    keep: Keep,
    capacity: Option(Int),
  )
}

pub fn new() -> Buffer(event) {
  Buffer(deque.new(), 0, Last, None)
}

pub fn keep(buffer: Buffer(event), keep: Keep) -> Buffer(event) {
  Buffer(..buffer, keep: keep)
}

pub fn capacity(buffer: Buffer(event), capacity: Int) -> Buffer(event) {
  use <- bool.guard(capacity <= 0, buffer)
  Buffer(..buffer, capacity: Some(capacity))
}

pub fn store(buffer: Buffer(event), events: List(event)) -> Buffer(event) {
  use <- bool.lazy_guard(at_capacity(buffer), fn() {
    store_at_capacity(buffer, events)
  })

  case events {
    [] -> buffer
    [event, ..rest] ->
      store(
        Buffer(
          ..buffer,
          queue: deque.push_back(buffer.queue, event),
          counter: buffer.counter + 1,
        ),
        rest,
      )
  }
}

fn at_capacity(buffer: Buffer(event)) -> Bool {
  case buffer.capacity {
    None -> False
    Some(capacity) -> buffer.counter > capacity
  }
}

fn store_at_capacity(
  buffer: Buffer(event),
  events: List(event),
) -> Buffer(event) {
  let popped = case buffer.keep {
    First -> deque.pop_back(buffer.queue)
    Last -> deque.pop_front(buffer.queue)
  }
  case popped {
    Error(Nil) -> buffer
    Ok(#(_, queue)) ->
      store(Buffer(..buffer, queue:, counter: buffer.counter - 1), events)
  }
}

pub fn estimate_count(buffer: Buffer(event)) -> Int {
  buffer.counter
}

pub type Take(event) {
  Take(buffer: Buffer(event), counter: Int, events: List(event))
}

pub fn take(buffer: Buffer(event), counter: Int) -> Take(event) {
  do_take(counter, [], buffer)
}

fn do_take(
  counter: Int,
  events: List(event),
  buffer: Buffer(event),
) -> Take(event) {
  use <- bool.lazy_guard(counter == 0, fn() {
    Take(buffer, counter, list.reverse(events))
  })

  case deque.pop_front(buffer.queue) {
    Error(Nil) ->
      Take(Buffer(..buffer, counter: 0), counter, list.reverse(events))

    Ok(#(event, queue)) ->
      do_take(
        counter - 1,
        [event, ..events],
        Buffer(..buffer, queue: queue, counter: buffer.counter - 1),
      )
  }
}
