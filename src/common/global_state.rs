use super::wait_manager::WaitManager;
use crate::{scheduler::Scheduler, storage::Database};
use config::Config;

pub struct GlobalState<'a> {
    config: Config,
    scheduler: Scheduler<'a>,
    database: Database,
    wait_manager: WaitManager,
}

impl<'a> GlobalState<'a> {
    pub fn new(
        config: Config,
        scheduler: Scheduler<'a>,
        database: Database,
        wait_manager: WaitManager,
    ) -> Self {
        Self {
            config,
            scheduler,
            database,
            wait_manager,
        }
    }

    pub fn get_config(&self) -> &Config {
        &self.config
    }

    pub fn get_scheduler(&self) -> &Scheduler<'a> {
        &self.scheduler
    }

    pub fn get_database(&self) -> &Database {
        &self.database
    }

    pub fn get_wait_manager(&self) -> &WaitManager {
        &self.wait_manager
    }
}
