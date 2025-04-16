import dolly/producer.{type Producer}
import gleam/option.{type Option, None, Some}

//In case of exits, the same reason is used to exit the consumer.
pub type Cancel {
  /// the consumer exits when the producer cancels or exits.
  Permanent
  /// then consumer exits only if reason is not :normal, :shutdown, or {:shutdown, reason}
  Transient
  /// the consumer never exits
  Temporary
}

pub type Subscription(event) {
  Subscription(
    to: Producer(event),
    cancel: Cancel,
    min_demand: Option(Int),
    max_demand: Int,
  )
}

pub fn to(producer: Producer(event)) -> Subscription(event) {
  Subscription(
    to: producer,
    cancel: Permanent,
    min_demand: None,
    max_demand: 1000,
  )
}

pub fn cancel(
  subscription: Subscription(event),
  cancel: Cancel,
) -> Subscription(event) {
  Subscription(..subscription, cancel:)
}

pub fn min_demand(
  subscription: Subscription(event),
  min_demand: Int,
) -> Subscription(event) {
  Subscription(..subscription, min_demand: Some(min_demand))
}

pub fn max_demand(
  subscription: Subscription(event),
  max_demand: Int,
) -> Subscription(event) {
  Subscription(..subscription, max_demand:)
}
