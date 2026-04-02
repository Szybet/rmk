use postcard::experimental::max_size::MaxSize;
use rmk_macro::event;
use rmk_types::action::Action;
use serde::{Deserialize, Serialize};

use crate::event::{AsyncPublishableEvent, KeyboardEvent, PublishableEvent, SubscribableEvent};

#[event(
    channel_size = crate::ACTION_EVENT_CHANNEL_SIZE,
    pubs = crate::ACTION_EVENT_PUB_SIZE,
    subs = crate::ACTION_EVENT_SUB_SIZE
)]
#[derive(Serialize, Deserialize, Clone, Copy, Debug, MaxSize, Eq, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ActionEvent {
    pub action: Action,
    pub keyboard_event: KeyboardEvent,
}

#[doc(hidden)]
static CONTROL_ACTION_EVENT_CHANNEL: ::embassy_sync::pubsub::PubSubChannel<
    crate::RawMutex,
    Action,
    { crate::CONTROL_ACTION_EVENT_CHANNEL_SIZE },
    { crate::CONTROL_ACTION_EVENT_SUB_SIZE },
    { crate::CONTROL_ACTION_EVENT_PUB_SIZE },
> = ::embassy_sync::pubsub::PubSubChannel::new();

impl PublishableEvent for Action {
    type Publisher = ::embassy_sync::pubsub::ImmediatePublisher<
        'static,
        crate::RawMutex,
        Action,
        { crate::CONTROL_ACTION_EVENT_CHANNEL_SIZE },
        { crate::CONTROL_ACTION_EVENT_SUB_SIZE },
        { crate::CONTROL_ACTION_EVENT_PUB_SIZE },
    >;
    const PUBLISH_IS_NOOP: bool = crate::CONTROL_ACTION_EVENT_SUB_SIZE == 0;

    fn publisher() -> Self::Publisher {
        CONTROL_ACTION_EVENT_CHANNEL.immediate_publisher()
    }
}

impl AsyncPublishableEvent for Action {
    type AsyncPublisher = ::embassy_sync::pubsub::Publisher<
        'static,
        crate::RawMutex,
        Action,
        { crate::CONTROL_ACTION_EVENT_CHANNEL_SIZE },
        { crate::CONTROL_ACTION_EVENT_SUB_SIZE },
        { crate::CONTROL_ACTION_EVENT_PUB_SIZE },
    >;

    fn publisher_async() -> Self::AsyncPublisher {
        CONTROL_ACTION_EVENT_CHANNEL
            .publisher()
            .expect("Failed to create async publisher for Action")
    }
}

impl SubscribableEvent for Action {
    type Subscriber = ::embassy_sync::pubsub::Subscriber<
        'static,
        crate::RawMutex,
        Action,
        { crate::CONTROL_ACTION_EVENT_CHANNEL_SIZE },
        { crate::CONTROL_ACTION_EVENT_SUB_SIZE },
        { crate::CONTROL_ACTION_EVENT_PUB_SIZE },
    >;

    fn subscriber() -> Self::Subscriber {
        CONTROL_ACTION_EVENT_CHANNEL
            .subscriber()
            .expect("Failed to create subscriber for Action")
    }
}
