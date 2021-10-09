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
    

// function getNeighbor (allActors: IActorRef[], index: int, topology:string)
    //match topolgy and return neighbors IActorRef[] accordingly
  

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
    let mutable neighbourCount = 0;
    let mutable neighbourList = [];
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Initalize(allActors, topology) ->
            //initialize neighbours list according to topology
            sprintf("Change this") |>ignore
        | Gossip ->
            gossipHeardOnce <- true
            timesGossipHeard <- timesGossipHeard + 1
            //if timesGossipHeard = 10 then
                
            //ifgossipReceived = false make true
            //increment numberGossipReceived
            //if numberGossipReceived=10
                //send Converged to parent
                //send Converged to Neighbours   
            
            
       
             
        //NeighborConverged
            //remove actor from list of neighbours as converged
            //decrement neighborCount and check if 0
            //send Converged to Parent

        |_->()
     
        //ifgossipReceived
            //keep sending gossip to random nodes in neighbours list
        
        
        return! loop()
    }
    loop()
    

//PushSum Actors
    //variables neighbourList, pushSumRecieved bool, int neighbourcount, int sum, int weight, int oldRatio, int lessthandelta
    //Initialize
        //initialize neighbours list according to topology
        //set lessthandelta = 0
        //set sum = i
        //set weight to 1
    //PushSum
        //pushSumRecieved = false make true
        //sum += newSum and weight+= newWeight
        //if |oldRatio - newRatio|<10**-10
            //increment lessthandelta
        //else
            //lessthandelta=0
        //if lessthandelta=3 then        
            //send Converged to parent
            //send Converged to Neighbours    
    //NeighborConverged
        //remove actor from list of neighbours as converged
        //decrement neighborCount and check if 0
        //send Converged to Parent

    //ifPushSumReceived
        //keep sending pushSum to random nodes in neighbours list (s/2,w/2)



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
        //Converged
        //increment number of actors that converged
        //if equal to total nodes
        //stop timer and terminate
        |_ -> ()
        return! loop()
    }
    loop()
    

//Spawn supervisor and start
let ActorSystem = System.create "bitcoin-miner-server" (Configuration.load())
let supervisor = spawn ActorSystem "supervisor" Supervisor
supervisor <! Start
ActorSystem.WhenTerminated.Wait()


        
