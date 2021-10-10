#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp

type SupervisorMsg = 
    | Start
    | Converged

type ActorMsg =
    | Initalize of allActors: List<IActorRef> * topology: string
    | NeighborConverged of IActorRef
    | Gossip
    | PushSum of sum:int * weight:int
    

//create gossip system
let gossipSystem = System.create "gossip-system" (Configuration.load())

// function getNeighbor (allActors: IActorRef[], index: int, topology:string)
let getNeighbors (actorName:IActorRef) (allActors:list<IActorRef>)(topology:string) =
    //match topolgy and return neighbors IActorRef[] accordingly
    let neighborList = []
    //change this according to topology
    let numberOfNeighbours = 10
    (neighborList, numberOfNeighbours)

// Round off to proper cubes for 3D and imperfect 3D
let timer = System.Diagnostics.Stopwatch()
let roundOffNodes (numNode:int) =
    //CHANGE THIS FOR 3D
    let mutable sqrtVal = numNode |> float |> sqrt |> int
    if sqrtVal*sqrtVal <> numNode then
        sqrtVal <- sqrtVal + 1
    sqrtVal*sqrtVal

//get number of nodes, topology, algo from command line
let mutable nodes = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2]
let algo = fsi.CommandLineArgs.[3]
let rand = Random(nodes)
if topology = "3D" || topology = "Imp3D" then
    nodes <- roundOffNodes nodes
//Make topology

//Gossip Actors
let GossipActor (mailbox: Actor<_>) =
    //variables neighbourList, numberGossipReceived, gossipReceived bool, int neighbourcount
    let mutable timesGossipHeard = 0
    let mutable gossipHeardOnce = false
    let mutable neighborCount = 0;
    let mutable neighborList = [];
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Initalize(allActors, topology) ->
            //initialize neighbours list according to topology
            let tupleValues = getNeighbors mailbox.Self allActors topology
            neighborList <- fst(tupleValues)
            neighborCount <- snd(tupleValues)
            sprintf("Change this") |>ignore
        | Gossip ->
            if gossipHeardOnce = false then
                gossipHeardOnce <- true
            //////SELECT RANDOM NEIGHBOR AND SEND GOSSIP
            //increment numberGossipReceived
            timesGossipHeard <- timesGossipHeard + 1
            if timesGossipHeard = 10 then  
                //send Converged to parent
                mailbox.Context.Parent <! Converged
                //send Converged to Neighbours  
                neighborList |> List.iter (fun item -> item <! NeighborConverged(mailbox.Self)     
        | NeighborConverged(actorRef) ->
                //remove actor from list of neighbours as converged
            //decrement neighborCount and check if 0
            neighborCount <- neighborCount - 1
            if neighborCount = 0 then
            //send Converged to Parent
            mailbox.Context.Parent <! Converged
       
             
        //ifgossipReceived
        if gossipHeardOnce then
            //////or we could use simple while loop and add delay of one second
            let randomNeighbor: IActorRef = neighborList.[rand.Next()%neighborCount]
            //keep sending gossip to random nodes in neighbours list
            gossipSystem.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), randomNeighbor, Gossip, mailbox.Self)
        
        return! loop()
    }
    loop()
    

//PushSum Actors
let PushSum (mailbox: Actor<_>) =
    //variables neighbourList, pushSumRecieved bool, int neighbourcount, int sum, int weight, int oldRatio, int lessthandelta
    let mutable lessthandelta = 0
    let mutable pushSumReceived = false
    let mutable neighborCount = 0
    let mutable neighborList = []
    let delta = 10.0 ** -10.0
    let mutable sum = 0
    let mutable weight = 1
    let mutable ratio = 0
    let mutable diff: float = 0.0
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Initalize(allActors, topology) ->
            //INITIALIZE NEIGHBOR LIST ACCORDING TO TOPOLOGY
            let tupleValues = getNeighbors mailbox.Self allActors topology
            neighborList <- fst(tupleValues)
            neighborCount <- snd(tupleValues)
            sprintf("Change this") |>ignore
            /////INITIALIZE SUM ACCORDING TO ACTOR NUMBER
            ratio <- sum/weight
        
        //PushSum
        |PushSum(newSum, newWeight) ->
            //pushSumRecieved = false make true
            if pushSumReceived = false then
                pushSumReceived <- true
            //sum += newSum and weight+= newWeight
            sum <- sum + newSum
            weight <- weight + newWeight
            //if |oldRatio - newRatio|<10**-10
            diff <- ratio - (sum/weight) |> float |> abs
            /////SEND SUM AND WEIGHT TO RANDOM NEIGHBOR
            //compute new ratio
            ratio <- sum/weight
            if diff > delta then    
                lessthandelta <- 0
            else
                lessthandelta <- (lessthandelta +1)
            
            //if lessthandelta=3 then  
            if lessthandelta = 3 then
                //send Converged to parent
                mailbox.Context.Parent <! Converged
                //send Converged to Neighbours  
                neighborList |> List.iter (fun item -> 
                    item <! NeighborConverged(mailbox.Self)) 
                    
        //NeighborConverged
        |NeighborConverged(actorRef) ->
            //remove actor from list of neighbours as converged
            //decrement neighborCount and check if 0
            neighborCount <- neighborCount - 1

        //ifPushSumReceived            
        if pushSumReceived then
            //////or we could use simple while loop and add delay of one second
            let randomNeighbor: IActorRef = neighborList.[rand.Next()%neighborCount]
            //keep sending gossip to random nodes in neighbours list
            gossipSystem.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), randomNeighbor, PushSum(sum, weight), mailbox.Self)
        return! loop()
    }
    loop()



//Supervisor
let Supervisor (mailbox:Actor<_>) =
    let mutable converged = 0
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Start ->
            //check topology
            if algo = "Gossip" then
                let allActors = 
                    [1 .. nodes]
                    |> List.map(fun id -> spawn mailbox.Context (sprintf "Actor_%d" id) GossipActor)
                allActors |> List.iter (fun item -> 
                    item <! Initalize(allActors, topology))
                //Start Timer
                timer.Start()
                //Send gossip to first node
                allActors.[(rand.Next()) % nodes] <! Gossip
        | Converged ->
            converged <- converged + 1
            if converged = nodes then
                printfn "Time taken = %i\n" timer.ElapsedMilliseconds
                mailbox.Context.Stop(mailbox.Self)
                mailbox.Context.System.Terminate() |> ignore
                
        return! loop()
    }
    loop()
    

//Spawn supervisor and start
let supervisor = spawn gossipSystem "supervisor" Supervisor
supervisor <! Start
gossipSystem.WhenTerminated.Wait()


        
