import gleam/bool
import gleam/list

pub type Batch(event) {
  Batch(events: List(event), size: Int)
}

pub type Demand {
  Demand(current: Int, min: Int, max: Int)
}

pub fn events(events: List(event), demand: Demand) -> #(Int, List(Batch(event))) {
  split_batches(
    events: events,
    min: demand.min,
    max: demand.max,
    old_demand: demand.current,
    new_demand: demand.current,
    batches: [],
  )
}

fn split_batches(
  events events: List(event),
  min min: Int,
  max max: Int,
  old_demand old_demand: Int,
  new_demand new_demand: Int,
  batches batches: List(Batch(event)),
) -> #(Int, List(Batch(event))) {
  use <- bool.lazy_guard(events == [], fn() {
    #(new_demand, list.reverse(batches))
  })

  let #(events, batch, batch_size) = split_events(events, max - min, 0, [])

  let #(old_demand, batch_size) = case old_demand - batch_size {
    diff if diff < 0 -> #(0, old_demand)
    diff -> #(diff, batch_size)
  }

  let #(new_demand, batch_size) = case new_demand - batch_size {
    diff if diff <= min -> #(max, max - diff)
    diff -> #(diff, 0)
  }

  split_batches(events, min, max, old_demand, new_demand, [
    Batch(batch, batch_size),
    ..batches
  ])
}

fn split_events(events: List(event), limit: Int, counter: Int, acc: List(event)) {
  use <- bool.lazy_guard(limit == counter, fn() {
    #(events, list.reverse(acc), counter)
  })

  case events {
    [] -> #([], list.reverse(acc), counter)
    [event, ..rest] -> {
      split_events(rest, limit, counter + 1, [event, ..acc])
    }
  }
}
