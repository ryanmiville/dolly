import dolly/producer.{type Producer}
import gleam/option.{type Option, None, Some}
import redux/otp/supervision.{type Restart}

pub type Subscription(event) {
  Subscription(
    to: Producer(event),
    restart: Restart,
    min_demand: Option(Int),
    max_demand: Int,
  )
}

pub fn to(producer: Producer(event)) -> Subscription(event) {
  Subscription(
    to: producer,
    restart: supervision.Permanent,
    min_demand: None,
    max_demand: 1000,
  )
}

pub fn restart(
  subscription: Subscription(event),
  restart: Restart,
) -> Subscription(event) {
  Subscription(..subscription, restart:)
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
