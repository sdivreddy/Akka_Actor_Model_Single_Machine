#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp

let system = ActorSystem.Create("Squares", Configuration.defaultConfig())
type Information = 
    | Info of (int64*int64*int64)
    | Done of (string)
    | Input of (int64*int64)

// Printer Actor - To print the output
let printerActor (mailbox:Actor<_>) = 
    let rec loop () = actor {
        let! (index:int64) = mailbox.Receive()
        printfn "%d" index      
        return! loop()
    }
    loop()
let printerRef = spawn system "Printer" printerActor

// Worker Actors - Takes input from Boss and do the processing using sliding window algo and returns the completed message.
let WorkerActor (mailbox:Actor<_>) =
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let boss = mailbox.Sender()
        match message with
        | Info(startind, k, endind) -> 
            let mutable sum = 0L

            for i in startind .. startind+k-1L do
                sum <- sum + (i*i)
                
            let value = sum |> double |> sqrt |> int64
            if sum = (value * value) then
                printerRef <! startind

            for i in startind+1L .. endind do
                sum <- sum + ((i+k-1L)*(i+k-1L)) - ((i-1L)*(i-1L))
                let value = sum |> double |> sqrt |> int64
                if sum = (value * value) then
                    printerRef <! i

            boss <! Done("Completed")
        | _ -> ()

        return! loop()
    }
    loop()
             
// Boss - Takes input from command line and spawns the actor pool. Splits the tasks based on cores count and allocates using Round-Robin
let BossActor (mailbox:Actor<_>) =
    let actcount = System.Environment.ProcessorCount |> int64
    let totalactors = actcount*125L
    let split = totalactors*1L
    let workerActorsPool = 
            [1L .. totalactors]
            |> List.map(fun id -> spawn system (sprintf "Local_%d" id) WorkerActor)

    let workerenum = [|for lp in workerActorsPool -> lp|]
    let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))
    let mutable completed = 0L

    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with 
        | Input(n,k) -> 
            let mutable startind = 1L
            let size = n/split
            for id in [1L .. split] do
                if id = actcount then
                    workerSystem <! Info(startind, k, n)
                else
                    workerSystem <! Info(startind, k, (startind + size - 1L))
                    startind <- startind + size
        | Done(complete) ->
            completed <- completed + 1L
            if completed = split then
                mailbox.Context.System.Terminate() |> ignore
        | _ -> ()
       
        return! loop()
    }
    loop()

let boss = spawn system "boss" BossActor
// Input from Command Line
let N = fsi.CommandLineArgs.[1] |> int64
let K = fsi.CommandLineArgs.[2] |> int64
boss <! Input(N, K)
// Wait until all the actors has finished processing
system.WhenTerminated.Wait()