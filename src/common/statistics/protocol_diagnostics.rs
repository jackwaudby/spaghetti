use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum ProtocolDiagnostics {
    Attendez(AttendezDiagnostics),
    Other,
}

impl ProtocolDiagnostics {
    pub fn new(protocol: &str) -> Self {
        match protocol {
            "attendez" => ProtocolDiagnostics::Attendez(AttendezDiagnostics::default()),
            _ => ProtocolDiagnostics::Other,
        }
    }

    pub fn merge(&mut self, other: ProtocolDiagnostics) {
        match self {
            ProtocolDiagnostics::Attendez(ref mut diag) => {
                if let ProtocolDiagnostics::Attendez(other_diag) = other {
                    diag.merge(&other_diag);
                } else {
                    panic!("do not match");
                }
            }
            _ => {}
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct AttendezDiagnostics {
    pub predecessors: usize,
}

impl AttendezDiagnostics {
    pub fn default() -> Self {
        Self { predecessors: 0 }
    }

    pub fn new(predecessors: usize) -> Self {
        Self { predecessors }
    }

    pub fn merge(&mut self, other: &AttendezDiagnostics) {
        self.predecessors += other.predecessors;
    }
}