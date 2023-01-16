use std::cmp::Ordering;

#[derive(Debug)]
pub enum Action {
    Set(i64),
    Incr(i64),
    Decr(i64),
    Mul(i64),
    Div(i64),
    If(Ordering, i64, Mutation),
    IfElse(Ordering, i64, Mutation, Mutation),
}

#[derive(Debug)]
pub struct Mutation {
    actions: Vec<Action>,
}

impl Mutation {
    pub(crate) fn new() -> Self {
        Mutation {
            actions: Vec::new(),
        }
    }

    pub fn set(mut self, val: i64) -> Self {
        self.actions.push(Action::Set(val));
        self
    }

    pub fn incr(mut self, val: i64) -> Self {
        self.actions.push(Action::Incr(val));
        self
    }

    pub fn decr(mut self, val: i64) -> Self {
        self.actions.push(Action::Decr(val));
        self
    }

    pub fn mul(mut self, val: i64) -> Self {
        self.actions.push(Action::Mul(val));
        self
    }

    pub fn div(mut self, val: i64) -> Self {
        debug_assert!(val != 0);
        self.actions.push(Action::Div(val));
        self
    }

    pub fn if_<F>(mut self, ord: Ordering, val: i64, f: F) -> Self
    where
        F: Fn(Mutation) -> Mutation,
    {
        self.actions.push(Action::If(ord, val, f(Mutation::new())));
        self
    }

    pub fn if_else<F, E>(mut self, ord: Ordering, val: i64, f: F, f2: E) -> Self
    where
        F: Fn(Mutation) -> Mutation,
        E: Fn(Mutation) -> Mutation,
    {
        self.actions.push(Action::IfElse(
            ord,
            val,
            f(Mutation::new()),
            f2(Mutation::new()),
        ));
        self
    }

    pub fn into_iter(self) -> impl Iterator<Item = Action> {
        self.actions.into_iter()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Action> {
        self.actions.iter()
    }

    pub fn len(&self) -> usize {
        self.actions.len()
    }
}
