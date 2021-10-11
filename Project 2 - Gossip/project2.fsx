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
    | StartPushSum
    

//create gossip system
let gossipSystem = System.create "gossip-system" (Configuration.load())

// function getNeighbor (allActors: IActorRef[], index: int, topology:string)
let getNeighbors (actorName:string) (allActors:list<IActorRef>) (topology:string) =

    //match topolgy and return neighbors IActorRef[] accordingly
    let mutable neighborList = []
    let mutable neighborIndices = []
    //change this according to topology
    //let numberOfNeighbours = 10
    let totalNodes = allActors.Length
    let currentNode = ((actorName.Split '_').[1] |> int) - 1


    let getThreeDNeighbors (n : int) =
        let gridLength = int <| Math.Cbrt(float totalNodes)
        let planeNodeCount = int (gridLength * gridLength)
        let currentPlane = (/) n planeNodeCount
        let planePosition = (%) n planeNodeCount
        let currentRow = (/) planePosition gridLength
        let currentColumn = (%) planePosition gridLength

        if currentPlane > 0 then
            neighborList <- neighborList @ [allActors.[n - planeNodeCount]]
            neighborIndices <- neighborIndices @ [n - planeNodeCount]
        if currentPlane < gridLength - 1 then
            neighborList <- neighborList @ [allActors.[n + planeNodeCount]]
            neighborIndices <- neighborIndices @ [n - planeNodeCount]

        if currentRow > 0 then
            neighborList <- neighborList @ [allActors.[n - gridLength]]
            neighborIndices <- neighborIndices @ [n - gridLength]
        if currentRow < gridLength - 1 then
            neighborList <- neighborList @ [allActors.[n + gridLength]]
            neighborIndices <- neighborIndices @ [n + gridLength]

        if currentColumn > 0 then
            neighborList <- neighborList @ [allActors.[n - 1]]
            neighborIndices <- neighborIndices @ [n - 1]
        if currentColumn < gridLength - 1 then
            neighborList <- neighborList @ [allActors.[n + 1]]
            neighborIndices <- neighborIndices @ [n + 1]

    

    match topology.ToLower() with
    |"line" ->
        if currentNode > 0 then
            neighborList <- allActors.[currentNode - 1] :: neighborList
        if currentNode < (totalNodes-1) then
            neighborList <- allActors.[currentNode + 1] :: neighborList
        

    |"full" ->
        neighborList <-
            allActors
            |> List.mapi (fun i el -> (i <> currentNode, el))
            |> List.filter fst |> List.map snd

    |"3d" ->
        getThreeDNeighbors currentNode

    |"imp3d" ->
        getThreeDNeighbors currentNode
        let rand = new Random()
        let mutable index = rand.Next(totalNodes-1); 
        while (index = currentNode || (List.contains index neighborIndices)) do
            index <- rand.Next(totalNodes-1)
        neighborList <- allActors.[index]:: neighborList

    |_->()

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
printfn "Beginning Algorithm for %s topology, %s protocol for %O nodes" topology algo nodes
let rand = new Random()

//Gossip Actors
let GossipActor (mailbox: Actor<_>) =
    
    //variables neighbourList, numberGossipReceived, gossipReceived bool, int neighbourcount
    let mutable timesGossipHeard: int = 0
    let mutable gossipHeardOnce: bool = false
    let mutable neighborCount: int = 0
    let mutable neighborList = []
    let parent = mailbox.Context.Parent
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Initalize(allActors, topology) ->
            //initialize neighbours list according to topology
            neighborList <- getNeighbors mailbox.Self.Path.Name allActors topology
            neighborCount <- neighborList.Length
        | Gossip ->           
            if gossipHeardOnce = false then
                gossipHeardOnce <- true
            // select random neighbor and sed gossip
            if neighborCount > 0 then
                let index: int = rand.Next(neighborCount)
                let randomNeighbor: IActorRef = neighborList.[index]
                randomNeighbor <! Gossip
            //increment numberGossipReceived
            timesGossipHeard <- timesGossipHeard + 1

            if timesGossipHeard = 10 then  
                //send Converged to parent
                parent <! Converged
                //send Converged to Neighbours
                neighborList |> List.iter (fun item -> item <! NeighborConverged(mailbox.Self))
                
        | NeighborConverged(actorRef) ->
            //remove actor from list of neighbours as converged
            neighborList <- neighborList |> List.filter ((<>) actorRef)
            //decrement neighborCount and check if 0
            neighborCount <- neighborCount - 1
        |_->()
             
        //ifgossipReceived
        if gossipHeardOnce && neighborCount> 0 then            
            let index: int = rand.Next(neighborCount)
            let randomNeighbor: IActorRef = neighborList.[index]
            //keep sending gossip to random nodes in neighbours list
            gossipSystem.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), randomNeighbor, Gossip, mailbox.Self)

        
        return! loop()
    }
    loop()
    

//PushSum Actors
let PushSumActor (mailbox: Actor<_>) =
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
            let actorName = mailbox.Self.Path.Name
            neighborList <- getNeighbors actorName allActors topology            
            neighborCount <- neighborList.Length
            /////INITIALIZE SUM ACCORDING TO ACTOR NUMBER
            sum <- ((actorName.Split '_').[1] |> int)
            ratio <- sum/weight
        | StartPushSum ->
            pushSumReceived <- true

            if neighborCount > 0 then
                let index = rand.Next(neighborCount)
                let randomNeighbor: IActorRef = neighborList.[index]
                randomNeighbor <! PushSum(sum, weight)


        //PushSum
        |PushSum(newSum, newWeight) ->
            //pushSumRecieved = false make true
            if pushSumReceived = false then
                pushSumReceived <- true
            //sum += newSum and weight+= newWeight
            sum <- sum + newSum
            weight <- weight + newWeight
            //if |oldRatio - newRatio|<10**-10
            diff <- (ratio - (sum/weight) |> float |> abs)
            /////SEND SUM AND WEIGHT TO RANDOM NEIGHBOR
            if neighborCount > 0 then
                let randomNeighbor: IActorRef = neighborList.[rand.Next(neighborCount)]
                randomNeighbor <! PushSum(sum, weight)
            //compute new ratio
            ratio <- sum/weight
            if diff > delta then    
                lessthandelta <- 0
            else
                lessthandelta <- (lessthandelta + 1)
            
            if lessthandelta = 3 then
                //send Converged to parent
                mailbox.Context.Parent <! Converged
                //send Converged to Neighbours  
                neighborList |> List.iter (fun item -> 
                    item <! NeighborConverged(mailbox.Self)) 
                    
        //NeighborConverged
        |NeighborConverged(actorRef) ->
            //remove actor from list of neighbours as converged
            neighborList <- neighborList |> List.filter ((<>) actorRef)
            //decrement neighborCount and check if 0
            neighborCount <- neighborCount - 1

        |_->()

        //ifPushSumReceived            
        if pushSumReceived && neighborCount> 0 then
            //////or we could use simple while loop and add delay of one second
            let randomNeighbor: IActorRef = neighborList.[rand.Next(neighborCount)]
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
            match algo with
            | "Gossip" ->
                let allActors = 
                    [1 .. nodes]
                    |> List.map(fun id -> spawn mailbox.Context (sprintf "Actor_%d" id) GossipActor)    
                allActors |> List.iter (fun item -> 
                    item <! Initalize(allActors, topology))
                //Start Timer                
                timer.Start()
                //Send gossip to first node
                let index = (rand.Next()) % nodes
                allActors.[index] <! Gossip
            | "PushSum" ->
                let allActors = 
                    [1 .. nodes]
                    |> List.map(fun id -> spawn mailbox.Context (sprintf "Actor_%d" id) PushSumActor)
                allActors |> List.iter (fun item -> 
                    item <! Initalize(allActors, topology))
                //Start Timer                
                timer.Start()
                //Send gossip to first node
                allActors.[(rand.Next()) % nodes] <! StartPushSum
            |_->()
        | Converged ->
            converged <- converged + 1
            if converged = nodes then
                printfn "Time taken to converge = %i ms\n" timer.ElapsedMilliseconds
                mailbox.Context.Stop(mailbox.Self)
                mailbox.Context.System.Terminate() |> ignore             
        return! loop()
    }
    loop()
    

//Spawn supervisor and start
let supervisor = spawn gossipSystem "supervisor" Supervisor
supervisor <! Start
gossipSystem.WhenTerminated.Wait()


        
