use basteh::dev::{Action, Mutation};

#[inline]
pub(crate) fn run_mutations(mut value: i64, mutations: Mutation) -> Option<i64> {
    for act in mutations.into_iter() {
        match act {
            Action::Set(rhs) => {
                value = rhs;
            }
            Action::Incr(rhs) => {
                value = value.checked_add(rhs)?;
            }
            Action::Decr(rhs) => {
                value = value.checked_sub(rhs)?;
            }
            Action::Mul(rhs) => {
                value = value.checked_mul(rhs)?;
            }
            Action::Div(rhs) => {
                value = value.checked_div(rhs)?;
            }
            Action::If(ord, rhs, sub) => {
                if value.cmp(&rhs) == ord {
                    value = run_mutations(value, sub)?;
                }
            }
            Action::IfElse(ord, rhs, sub, sub2) => {
                if value.cmp(&rhs) == ord {
                    value = run_mutations(value, sub)?;
                } else {
                    value = run_mutations(value, sub2)?;
                }
            }
        }
    }
    Some(value)
}
