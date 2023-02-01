use std::{cmp::Ordering, fmt::Write};

use basteh::dev::Action;
use redis::{aio::ConnectionManager, RedisError, Script};

pub(super) async fn run_mutations(
    mut con: ConnectionManager,
    key: Vec<u8>,
    mutations: impl IntoIterator<Item = Action>,
) -> std::result::Result<(), RedisError> {
    let (script, args) = make_script(mutations);

    let script = Script::new(&script);
    let mut args = args.into_iter();

    let mut script = script.arg(args.next().unwrap());

    for arg in args {
        script.arg(arg);
    }

    script.key(key).invoke_async(&mut con).await
}

fn make_script(mutations: impl IntoIterator<Item = Action>) -> (String, Vec<i64>) {
    let mut script = String::new();
    let mut args = Vec::new();
    script.push_str("local r=redis.call('GET', KEYS[1])\n");

    write_operation(mutations, &mut script, &mut args);

    script.push_str("redis.call('SET', KEYS[1], r)\n");

    (script, args)
}

fn write_operation(
    mutations: impl IntoIterator<Item = Action>,
    script: &mut String,
    args: &mut Vec<i64>,
) {
    for act in mutations.into_iter() {
        match act {
            Action::Set(arg) => {
                args.push(arg);

                script.push_str("r = tonumber(ARGV[");
                script.push_str(&args.len().to_string());
                script.push_str("])\n");
            }
            Action::Incr(arg) => {
                args.push(arg);

                script.push_str("r = tonumber(r) + tonumber(ARGV[");
                script.push_str(&args.len().to_string());
                script.push_str("])\n");
            }
            Action::Decr(arg) => {
                args.push(arg);

                script.push_str("r = tonumber(r) - tonumber(ARGV[");
                script.push_str(&args.len().to_string());
                script.push_str("])\n");
            }
            Action::Mul(arg) => {
                args.push(arg);

                script.push_str("r = tonumber(r) * tonumber(ARGV[");
                script.push_str(&args.len().to_string());
                script.push_str("])\n");
            }
            Action::Div(arg) => {
                args.push(arg);

                script.push_str("r = tonumber(r) / tonumber(ARGV[");
                script.push_str(&args.len().to_string());
                script.push_str("])\n");
            }
            Action::If(ord, arg, sub) => {
                args.push(arg);

                let operator = match ord {
                    Ordering::Equal => "==",
                    Ordering::Greater => ">",
                    Ordering::Less => "<",
                };

                write!(
                    script,
                    "if(tonumber(r) {} tonumber(ARGV[{}])) then \n",
                    operator,
                    args.len()
                )
                .unwrap();

                write_operation(sub.into_iter(), script, args);

                script.push_str("end\n");
            }
            Action::IfElse(ord, arg, sub, sub2) => {
                args.push(arg);

                let operator = match ord {
                    Ordering::Equal => "==",
                    Ordering::Greater => ">",
                    Ordering::Less => "<",
                };

                write!(
                    script,
                    "if(tonumber(r) {} tonumber(ARGV[{}])) then \n",
                    operator,
                    args.len()
                )
                .unwrap();

                write_operation(sub.into_iter(), script, args);

                script.push_str("else\n");

                write_operation(sub2.into_iter(), script, args);

                script.push_str("end\n");
            }
        }
    }
}
