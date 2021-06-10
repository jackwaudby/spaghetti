use crate::common::error::NonFatalError;
use crate::storage::datatype::{Data, Field};

use std::fmt;

use std::cell::UnsafeCell;

unsafe impl Sync for Tuple {}

#[derive(Debug)]
pub struct Tuple(UnsafeCell<Internal>);

impl Tuple {
    pub fn new() -> Self {
        Tuple(UnsafeCell::new(Internal::new()))
    }

    pub fn get(&self) -> &mut Internal {
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
        self.state == State::Modified
    }

    pub fn init_value(&mut self, value: Data) -> Result<(), NonFatalError> {
        self.current.set(value);
        Ok(())
    }

    pub fn get_value(&self) -> Result<OpResult, NonFatalError> {
        Ok(OpResult::new(Some(self.current.get())))
    }

    pub fn set_value(&mut self, value: &Data) -> OpResult {
        match self.state {
            State::Modified =>
            //Err(NonFatalError::RowDirty),
            {
                panic!("row dirty")
            }
            State::Clean => {
                self.state = State::Modified; // set state
                let prev = self.current.clone(); // set prev fields
                self.prev = Some(prev);
                self.current.set(value.clone());

                OpResult::new(None)
                //  Ok(OpResult::new(None))
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

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TODO")
    }
}
