import dolly/buffer.{Take}
import gleeunit/should

pub fn buffer_drops_events_when_at_capacity_test() {
  let buffer =
    buffer.new()
    |> buffer.capacity(1)
    |> buffer.store([1, 2, 3])

  let Take(_, _, events) = buffer.take(buffer, 3)
  events |> should.equal([3])
}

pub fn buffer_keep_first_test() {
  let buffer =
    buffer.new()
    |> buffer.capacity(1)
    |> buffer.keep(buffer.First)
    |> buffer.store([1, 2, 3])

  let Take(_, _, events) = buffer.take(buffer, 3)
  events |> should.equal([1])
}
