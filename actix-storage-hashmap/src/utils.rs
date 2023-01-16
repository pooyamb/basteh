use actix_storage::dev::{Action, Mutation};

pub(crate) fn run_mutations(mut value: i64, mutations: Mutation) -> i64 {
    for act in mutations.into_iter() {
        match act {
            Action::Set(rhs) => {
                value = rhs;
            }
            Action::Incr(rhs) => {
                value = value + rhs;
            }
            Action::Decr(rhs) => {
                value = value - rhs;
            }
            Action::Mul(rhs) => {
                value = value * rhs;
            }
            Action::Div(rhs) => {
                value = value / rhs;
            }
            Action::If(ord, rhs, sub) => {
                if value.cmp(&rhs) == ord {
                    value = run_mutations(value, sub);
                }
            }
            Action::IfElse(ord, rhs, sub, sub2) => {
                if value.cmp(&rhs) == ord {
                    value = run_mutations(value, sub);
                } else {
                    value = run_mutations(value, sub2);
                }
            }
        }
    }
    value
}
