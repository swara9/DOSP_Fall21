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
let getNeighbors (actorName:string) (allActors:list<IActorRef>) (topology:string) =
    //match topolgy and return neighbors IActorRef[] accordingly
    let mutable neighborList = []
    //change this according to topology
    //let numberOfNeighbours = 10
    let totalNodes = allActors.Length
    let currentNode = (actorName.[actorName.Length - 1] |> int) - 1

    let getThreeDNeighbors (n : int) =
        let gridLength = int <| Math.Cbrt(float totalNodes)
        let planeNodeCount = int (gridLength * gridLength)
        let currentPlane = (/) n planeNodeCount
        let planePosition = (%) n planeNodeCount
        let currentRow = (/) planePosition gridLength
        let currentColumn = (%) planePosition gridLength

        if currentPlane > 0 then
            neighborList <- neighborList @ [allActors.[n - planeNodeCount]]
        if currentPlane < gridLength - 1 then
            neighborList <- neighborList @ [allActors.[n + planeNodeCount]]

        if currentRow > 0 then
            neighborList <- neighborList @ [allActors.[n - gridLength]]
        if currentRow < gridLength - 1 then
            neighborList <- neighborList @ [allActors.[n + gridLength]]

        if currentColumn > 0 then
            neighborList <- neighborList @ [allActors.[n - 1]]
        if currentColumn < gridLength - 1 then
            neighborList <- neighborList @ [allActors.[n + 1]]

    match topology.ToLower() with
    |"line" ->
        if currentNode > 0 then
            neighborList <- neighborList @ [allActors.[currentNode - 1]]
        if currentNode < (totalNodes - 2) then
            neighborList <- neighborList @ [allActors.[currentNode + 1]]

    |"full" ->
        neighborList <-
            allActors
            |> List.mapi (fun i el -> (i <> currentNode, el))
            |> List.filter fst |> List.map snd

    |"3d" ->
        getThreeDNeighbors currentNode

    |"Imp3D" ->
        getThreeDNeighbors currentNode
        let mutable randomNode = Random().Next(0, totalNodes)
        while randomNode = currentNode do
            randomNode <- Random().Next(0, totalNodes)
        neighborList <- neighborList @ [allActors.[randomNode]]

    neighborList

// Round off to proper cubes for 3D and imperfect 3D
let timer = System.Diagnostics.Stopwatch()
let roundOffNodes (numNode:int) =
    let gridLength = int <| Math.Cbrt(float numNode)
    (float gridLength ** 3.0) |> int

//get number of nodes, topology, algo from command line
let mutable nodes = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2]
let algo = fsi.CommandLineArgs.[3]
if topology = "3D" || topology = "Imp3D" then
    nodes <- roundOffNodes nodes
printfn "%O" nodes
let rand = Random(nodes)
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
            neighborList <- getNeighbors mailbox.Self.Path.Name allActors topology
            neighborCount <- neighborList.Length
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
                neighborList |> List.iter (fun item -> item <! NeighborConverged(mailbox.Self))
                
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
            neighborList <- getNeighbors mailbox.Self.Path.Name allActors topology
            //neighborList <- fst(tupleValues)
            neighborCount <- neighborList.Length
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
                lessthandelta <- (lessthandelta + 1)
            
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
printfn "Spawning Supervisor"
supervisor <! Start
gossipSystem.WhenTerminated.Wait()


        
