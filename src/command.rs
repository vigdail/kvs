use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}
