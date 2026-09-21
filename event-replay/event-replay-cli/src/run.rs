use std::path::PathBuf;

use event_replay_config::Host;
use event_replay_core::window::Window;

use crate::change_log::ChangeLogFiles;
use crate::credential::Credential;
use crate::journal::Journal;
use crate::unreplayable::Unreplayable;

pub struct Run {
    directory: PathBuf,
    window: Window,
    credential: Credential,
}

impl Run {
    pub fn new(directory: PathBuf, window: Window, credential: Credential) -> Self {
        Self {
            directory,
            window,
            credential,
        }
    }

    pub fn report(&self) -> Result<Journal, Unreplayable> {
        let host = Host::open(&self.directory)?;
        let files =
            ChangeLogFiles::of(&host, &self.credential).ok_or(Unreplayable::WithoutReadableSink)?;

        files.report(&self.window)
    }
}
