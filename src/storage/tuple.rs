use crate::common::error::NonFatalError;
use crate::storage::datatype::{Data, Field};

use std::cell::UnsafeCell;
use std::fmt;

unsafe impl Sync for Tuple {}

#[derive(Debug)]
pub struct Tuple(UnsafeCell<Internal>);

impl Tuple {
    pub fn new() -> Self {
        Tuple(UnsafeCell::new(Internal::new()))
    }

    pub fn get(&self) -> &mut Internal {
        // Safety: 'correct' access to a tuple is managed by the concurrency control protocol.
        unsafe { &mut *self.0.get() }
    }
}

#[derive(Debug)]
pub struct Internal {
    current: Field,
    pub prev: Option<Field>,
    pub state: State,
}

#[derive(Debug, Clone, PartialEq)]
pub enum State {
    Clean,
    Modified,
}

#[derive(Debug)]
pub struct OpResult {
    value: Option<Data>,
}

impl Internal {
    pub fn new() -> Self {
        Internal {
            current: Field::new(),
            prev: None,
            state: State::Clean,
        }
    }

    pub fn is_dirty(&self) -> bool {
        if let State::Modified = self.state {
            true
        } else {
            false
        }
    }

    pub fn init_value(&mut self, value: Data) -> Result<(), NonFatalError> {
        self.current.set(value);

        Ok(())
    }

    pub fn get_value(&self) -> Result<OpResult, NonFatalError> {
        Ok(OpResult::new(Some(self.current.get())))
        // Ok(OpResult::new(None))
    }

    pub fn set_value(&mut self, value: &mut Data) -> Result<OpResult, NonFatalError> {
        match self.state {
            State::Modified => Err(NonFatalError::RowDirty("TODO".to_string())),

            State::Clean => {
                self.state = State::Modified; // set state
                let prev = self.current.clone(); // set prev fields
                self.prev = Some(prev);

                if let Data::List(ref mut item) = value {
                    if let Data::List(mut current) = self.current.clone().get() {
                        current.append(item);

                        self.current.set(Data::List(current));
                    }
                } else {
                    self.current.set(value.clone());
                }
                Ok(OpResult::new(None))
            }
        }
    }

    pub fn commit(&mut self) {
        self.prev = None;
        self.state = State::Clean;
    }

    pub fn revert(&mut self) {
        match self.state {
            State::Modified => {
                self.current = self.prev.take().unwrap(); // revert to old values
                self.state = State::Clean;
            }
            State::Clean => {}
        }
    }

    pub fn get_state(&self) -> State {
        self.state.clone()
    }
}

impl OpResult {
    pub fn new(value: Option<Data>) -> Self {
        OpResult { value }
    }

    pub fn get_value(&mut self) -> Data {
        self.value.take().unwrap()
        // Data::Uint(0)
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            State::Clean => write!(f, "clean"),
            State::Modified => write!(f, "dirty"),
        }
    }
}

impl fmt::Display for Internal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f).unwrap();
        writeln!(f, "---tuple---").unwrap();
        writeln!(f, "current: {}", self.current).unwrap();
        writeln!(f, "prev: {:?}", self.prev).unwrap();
        writeln!(f, "state: {}", self.state).unwrap();
        writeln!(f, "-----------").unwrap();
        Ok(())
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.get())
    }
}
