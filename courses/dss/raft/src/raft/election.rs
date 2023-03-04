use crate::raft::{NodeId, TermId};
use assert2::assert;

#[derive(Default, Debug)]
pub struct Election {
    term: TermId,
    pub voted_for: Option<NodeId>,
}

impl Election {
    pub fn get_current_term(&self) -> TermId {
        self.term
    }

    pub fn update_current_term(&mut self, new_term: TermId) {
        assert!(new_term >= self.term);
        self.term = new_term;
    }

    pub fn increment_term(&mut self) {
        self.term += 1;
    }
}
