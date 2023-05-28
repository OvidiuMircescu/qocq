use async_process::Command;
use futures::future::FutureExt;
use async_std::task;
use futures::future;
use core::pin::Pin;

// exchange type of arguments
type ArgType = u32; //TODO define the actual type

// The result of a remote evaluation may be a value (ArgType) or an error (String)
// TODO deal with multiple results
type EvaluationResult = Result<ArgType, String>;

//
type FutureEvaluationRef = Pin<Box<dyn futures::Future<Output = EvaluationResult> + Send >>;

// Task evaluated locally and concurrently, used to spawn remote tasks.
// It should be used to implement blocs, foreach, forloop, conditions.
type SyncComposedTaskType = Box<dyn Fn(ArgType)-> EvaluationResult  + Send >;

pub enum AsyncCommand {
    Local(SyncComposedTaskType),
    Remote(String) //TODO type of remote command
}

pub async fn async_call(command:AsyncCommand, args:FutureEvaluationRef) -> EvaluationResult
{
    let sync_args = args.boxed().await?;
    match command{
        AsyncCommand::Local(func) => {
            async move {func(sync_args)}.await
        },
        AsyncCommand::Remote(command_name) => {
            let ret = Command::new(command_name).arg(sync_args.to_string()).status().await;
            match ret{
                Err(e) => Err(e.to_string()),
                Ok(_) => Ok(sync_args)
            }
        }
    }
}

pub fn gather<Fut>(vals:Vec<Fut>) -> Vec<EvaluationResult>
where
    Fut: future::Future<Output = EvaluationResult>
{
    task::block_on(future::join_all(vals))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    fn composed_task(args:ArgType)->EvaluationResult{
        let r1 = async_call(
            AsyncCommand::Remote("sleep".to_string()),
            Box::pin(async move{Ok(args)})).shared();
        let r2 = async_call(
            AsyncCommand::Remote("sleep".to_string()),
            Box::pin(r1.clone())).shared();
        let r3 = async_call(
                AsyncCommand::Remote("sleep".to_string()),
                Box::pin(r1.clone())).shared();
        let lst = vec![r1, r2, r3];
        gather(lst);
        Ok(42)
    }

    #[test]
    fn synctest(){
        let start_time = Instant::now();
        let r1 = async_call(
            AsyncCommand::Remote("sleep".to_string()),
            Box::pin(async{Ok(1)})).shared();
        let r2 = async_call(
            AsyncCommand::Remote("sleep".to_string()),
            Box::pin(r1.clone())).shared();
        let r3 = async_call(
                AsyncCommand::Remote("sleep".to_string()),
                Box::pin(r1.clone())).shared();
        let r4 = async_call(
            AsyncCommand::Remote("sleep".to_string()),
            Box::pin(r2.clone())).shared();
        let r5 = async_call(
            AsyncCommand::Local(Box::new(composed_task)),
             Box::pin(r4.clone())).shared();
        let lst: Vec<_> = vec![r1, r2, r3, r4, r5];
        let res = gather(lst);

        let run_time = start_time.elapsed().as_secs();
        assert!(run_time == 5);
        assert!(res[1] == Ok(1));
        assert!(res[4] == Ok(42));

        // for r in res{
        //     match r{
        //         Ok(v) => println!("value:{v}"),
        //         Err(e) => println!("error:{e}")
        //     };
        // }
        // println!("elapsed:{run_time}");
    }

}
