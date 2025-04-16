import dolly/consumer.{type Consumer}
import dolly/internal/message
import dolly/subscription.{type Subscription}
import gleam/option
import redux/erlang/process

pub fn subscribe(
  from consumer: Consumer(event),
  to subscription: Subscription(event),
) -> Nil {
  process.send(consumer.subject, subscribe_message(subscription))
}

fn subscribe_message(subscription: Subscription(event)) {
  let min_demand =
    option.unwrap(subscription.min_demand, subscription.max_demand / 2)

  // TODO remove jank
  let cancel = case subscription.cancel {
    subscription.Permanent -> message.Permanent
    subscription.Temporary -> message.Temporary
    subscription.Transient -> message.Transient
  }
  message.ConsumerSubscribe(
    subscription.to.subject,
    cancel,
    min_demand,
    subscription.max_demand,
  )
}
