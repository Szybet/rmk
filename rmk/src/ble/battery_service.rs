use embassy_sync::pubsub::Subscriber;
use embassy_time::{Duration, Timer, with_timeout};
use trouble_host::prelude::*;

use super::ble_server::Server;
use crate::event::{BatteryStateEvent, SubscribableEvent};

/// Battery service
#[gatt_service(uuid = service::BATTERY)]
pub(crate) struct BatteryService {
    /// Battery Level
    #[descriptor(uuid = descriptors::VALID_RANGE, read, value = [0, 100])]
    #[characteristic(uuid = characteristic::BATTERY_LEVEL, read, notify)]
    pub(crate) level: u8,
}

pub(crate) struct BleBatteryServer<'stack, 'server, 'conn, P: PacketPool> {
    pub(crate) battery_level: Characteristic<u8>,
    pub(crate) conn: &'conn GattConnection<'stack, 'server, P>,
    pub(crate) sub: Subscriber<
        'static,
        crate::RawMutex,
        BatteryStateEvent,
        { crate::BATTERY_STATE_EVENT_CHANNEL_SIZE },
        { crate::BATTERY_STATE_EVENT_SUB_SIZE },
        { crate::BATTERY_STATE_EVENT_PUB_SIZE },
    >,
    pub(crate) state: BatteryStateEvent,
}

impl<'stack, 'server, 'conn, P: PacketPool> BleBatteryServer<'stack, 'server, 'conn, P> {
    pub(crate) fn new(server: &Server, conn: &'conn GattConnection<'stack, 'server, P>) -> Self {
        Self {
            battery_level: server.battery_service.level,
            conn,
            sub: BatteryStateEvent::subscriber(),
            state: BatteryStateEvent::NotAvailable,
        }
    }
}

impl<P: PacketPool> BleBatteryServer<'_, '_, '_, P> {
    pub(crate) async fn run(&mut self) {
        // Wait 2 seconds, ensure that gatt server has been started
        Timer::after_secs(2).await;

        // First report after connected
        let first_report = async {
            loop {
                if let BatteryStateEvent::Normal(level) = self.sub.next_message_pure().await {
                    if let Err(e) = self.battery_level.notify(self.conn, &level).await {
                        error!("Failed to notify battery level: {:?}", e);
                    } else {
                        return;
                    }
                }
                embassy_time::Timer::after_secs(2).await;
            }
        };

        // Try to do the first battery report in 30 seconds
        with_timeout(Duration::from_secs(30), first_report).await.ok();

        // Report the battery level.
        loop {
            self.wait_until_battery_state_available().await;

            // Drain latest messages
            while let Some(s) = self.sub.try_next_message_pure() {
                self.state = s;
            }

            if let BatteryStateEvent::Normal(level) = self.state {
                if let Err(e) = self.battery_level.notify(self.conn, &level).await {
                    error!("Failed to notify battery level: {:?}", e);
                }
            }
        }
    }

    /// Wait until the battery state is available.
    /// To avoid unexpected wakeup, before reporting battery level, all conditions should be satistied:
    ///
    /// 1. There's a battery state update
    /// 2. There's a key press in last 1 minute, or timeout(30 minutes)
    /// 3. The keyboard is not in the sleep mode
    async fn wait_until_battery_state_available(&mut self) {
        loop {
            self.state = self.sub.next_message_pure().await;
            if self.state == BatteryStateEvent::NotAvailable {
                continue;
            } else {
                return;
            }
        }
    }
}
