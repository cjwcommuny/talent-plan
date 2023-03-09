use crate::raft::{NodeId, TermId};
use assert2::assert;
use derive_new::new;

#[derive(Default, Debug, new)]
pub struct Election {
    term: TermId,
    pub voted_for: Option<NodeId>,
}

impl Election {
    pub fn current_term(&self) -> TermId {
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
