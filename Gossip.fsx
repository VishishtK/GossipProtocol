#r "nuget: Akka.Fsharp"

open System
open Akka.Actor
open Akka.FSharp

type Message =
    | Rumor of string
    | Converge of string
    | Gossip of string
    | Update of int
    | PushSum of float * float
    | SumConverge of float

type Algo =
    | PushSum
    | Gossip

type Node = 
    | LineNode of int
    | Grid3dNode of int * int * int
    | Grid3dImperfectNode of int * int * int
    | FullNode of int

type Topology = 
    | Line
    | Grid3d
    | Grid3dImperfect
    | Full

type Supervisor = 
    | RumorReceived
    | SumAchieved

type Worker = 
    | Self
    | Rumor
    | SumWeight of float * float

let n = 10
// let n = 27
// let n = 64
// let n = 100
// let n = 125
// let n = 216
// let n = 343
// let n = 512
// let n = 729
// let n = 1000

let algo = PushSum
let topology = Full
let gossipCount = 10

let delta = Math.Pow(10.0,-10.0)
printfn "%A" delta

let system = ActorSystem.Create("FSharp")
let timer = Diagnostics.Stopwatch()
let random = Random()

let accomadateN = 
    match topology with
    | Line | Full-> n
    | Grid3d | Grid3dImperfect-> Math.Pow(float(int (Math.Pow(float n, 0.333))+ 1 ), 3.0) |> int
    
let pickRandom (l: List<_>) =
    let r = Random()
    l.[r.Next(l.Length)]

let getActorFromNode node= 
    match node with
    | LineNode x ->
        let name = "Line" + string x
        let actorPath = @"akka://FSharp/user/" + name
        select actorPath system
    | Grid3dNode (x,y,z) ->
        let name = "Grid3d_x" + string x + "_y" + string y + "_z" + string z
        let actorPath = @"akka://FSharp/user/" + name
        select actorPath system
    | Grid3dImperfectNode (x,y,z) -> 
        let name = "Grid3dImperfect_x" + string x + "_y" + string y + "_z" + string z
        let actorPath = @"akka://FSharp/user/" + name
        select actorPath system
    | FullNode x -> 
        let name = "Full" + string x
        let actorPath = @"akka://FSharp/user/" + name
        select actorPath system
    

let getNeighbours node = 
    match node with
    | LineNode x ->
        if x = 1
            then [LineNode 2]
        elif x = n
            then [LineNode (x-1)]
        else [LineNode (x-1); LineNode (x+1)]
    | Grid3dNode (x,y,z) ->[
            if x-1>=0
            then Grid3dNode (x-1,y,z)
            if x+1<=accomadateN
            then Grid3dNode(x+1,y,z)
            if y-1>=0
            then Grid3dNode (x,y-1,z)
            if y+1<=accomadateN
            then Grid3dNode (x,y+1,z)
            if y-1>=0
            then Grid3dNode (x,y,z-1)
            if y+1<=accomadateN
            then Grid3dNode (x,y,z+1)
        ]
    | Grid3dImperfectNode (x,y,z) -> [
            if x-1>=0
            then Grid3dImperfectNode (x-1,y,z)
            if x+1<=accomadateN
            then Grid3dImperfectNode(x+1,y,z)
            if y-1>=0
            then Grid3dImperfectNode (x,y-1,z)
            if y+1<=accomadateN
            then Grid3dImperfectNode (x,y+1,z)
            if y-1>=0
            then Grid3dImperfectNode (x,y,z-1)
            if y+1<=accomadateN
            then Grid3dImperfectNode (x,y,z+1)
            Grid3dImperfectNode (random.Next accomadateN,random.Next accomadateN,random.Next accomadateN)
        ]
    | FullNode x -> [
            for i in [1..n] do  
                if i<>x
                then FullNode i
        ]

let Supervisor(mailbox: Actor<_>) =
    let rec loop convergedNodes ()= actor{
        let! msg = mailbox.Receive();
        let sender = mailbox.Sender()

        if convergedNodes = accomadateN-1
        then 
            printfn "Completed" 
            timer.Stop()
            printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
            exit 0
        
        if convergedNodes < accomadateN
        then 
            match msg with 
            | RumorReceived -> 
                // printfn "converged %A" sender 
                return! loop (convergedNodes + 1) ()
            | SumAchieved -> 
                // printfn "converged %A" sender
                return! loop (convergedNodes + 2) ()
    }           
    loop 0 ()

let supervisorRef = spawn system "supervisor" Supervisor

let Actor node i (mailbox: Actor<_>) =
    let neighbours = getNeighbours node
    let mutable count = 0
    let mutable s = float i
    let mutable w = float 1
    let mutable noDeltaChangeIter = 0
    let rec loop ()= actor{
        let! msg = mailbox.Receive();
        match algo with
        | PushSum -> 
            match msg with
            | SumWeight (sum,weight) -> 
                if noDeltaChangeIter < 3
                then 
                    let sNew = s + sum
                    let wNew = w + weight
                    let actor = pickRandom neighbours
                    getActorFromNode actor <! SumWeight(sNew/2.0,wNew/2.0)
                    let cal = float (s/w) - float(sNew / wNew) |> abs
                    s<-sNew/2.0
                    w<-wNew/2.0
                    if (float cal) < delta
                    then 
                        noDeltaChangeIter <- noDeltaChangeIter + 1
                    else 
                        noDeltaChangeIter <- 0
                    
                    if noDeltaChangeIter = 3
                    then 
                        supervisorRef <! SumAchieved

                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self , Self)
                return! loop ()
            | Self ->
                let actor = pickRandom neighbours
                s<-s/2.0
                w<-w/2.0
                getActorFromNode actor <! SumWeight(s,w)
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self , Self)
                return! loop ()
            | Rumor -> ()
        | Gossip -> 
            match msg with
            | Rumor -> 
                if count = 0
                then 
                    supervisorRef <! RumorReceived 
                
                if count < gossipCount
                then
                    let actor = pickRandom neighbours
                    getActorFromNode actor <! Rumor
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self , Self)
                    count <- count + 1
                    return! loop ()
            | Self ->
                    let actor = pickRandom neighbours
                    getActorFromNode actor <! Rumor
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self , Self)
                    return! loop ()
            | SumWeight (s,w) -> ()
    }            
    loop ()

let nodes = 
    let cube = int (Math.Pow(float n, 0.333))+ 1 
    let mutable count = 0
    match topology with 
    | Line -> [1..n]
            |> List.map(fun x -> 
                let name = "Line" + string x
                spawn system name (Actor (LineNode x) x)
            )
    | Grid3d -> [
                    for x in 1..cube do
                        for y in 1..cube do
                            for z in 1..cube do
                                let name = "Grid3d_x" + string x + "_y" + string y + "_z" + string z
                                spawn system name (Actor (Grid3dNode (x,y,z)) count)
                                count <- count + 1
                ]
    | Grid3dImperfect -> [
                            for x in 1..cube do
                                for y in 1..cube do
                                    for z in 1..cube do
                                        let name = "Grid3dImperfect_x" + string x + "_y" + string y + "_z" + string z
                                        spawn system name (Actor (Grid3dImperfectNode (x,y,z)) count)
                                        count <- count + 1
                        ]
    | Full -> [1..n]
            |> List.map(fun x -> 
                let name = "Full" + string x
                spawn system name (Actor (FullNode x) x)
   
            )

let starterActor =  pickRandom nodes
timer.Start()
match algo with 
| PushSum -> starterActor <! SumWeight(0.0,0.0)
| Gossip -> starterActor <! Rumor

Console.ReadLine() |> ignore