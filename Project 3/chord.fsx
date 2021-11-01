#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Collections.Generic

let num_nodes = 10
let num_message = 10
let hash_length = 160
let chord_system = System.create "chord-system" (Configuration.load())
let maxNode:bigint = bigint(2.0 ** 160.0)

type Supervisor_Messge = 
    | Start of nodes:int * messages:int
    | CreateNewNode of node: string
    | Init_Done
    | Received_Message of no_of_hops: int

type Node_Message =
    | Initialize of chord_nodes: bigint [] * current_index: int
    // | Begin_Simulation
    // | Route of message: string * hop: int
    | FindSuccessor of id: string * askerNode: bigint * purpose: string
    | ChangeSuccessor of succesor: string
    | Create
    | Join of node: string
    | Stabilize of predOfSucc: string
    | TellYourPred
    | Notify of n : string
    | CheckPred of state : string
    | TellState
    | FixFingers
    | UpdateFinger of value: bigint * index: int 
let hash_type = "SHA1"

let hash_string (input: string, algo: string) =
    let hash_bytes = input 
                        |> System.Text.Encoding.UTF8.GetBytes
                        |> HashAlgorithm.Create(algo).ComputeHash
    let hash_string = "0" + 
                        (hash_bytes
                        |> Seq.map (fun c -> c.ToString "x2")
                        |> Seq.reduce (+))
    hash_string


let ranStr n = 
    let r = Random()
    let chars = Array.concat([[|'a' .. 'z'|];[|'A' .. 'Z'|];[|'0' .. '9'|]])
    let sz = Array.length chars in
    String(Array.init n (fun _ -> chars.[r.Next sz]))

let convertToBigInt hash =
    bigint.Parse(hash, System.Globalization.NumberStyles.HexNumber)

let Chord_Node (mailbox : Actor<_>) =
    // let mutable finger_table = Array2D.init hash_length 2 (fun _ _ -> "")
    let mutable finger_table = new List<string>()
    let mutable predecessor:bigint = -1.0 |>bigint
    let currNode = mailbox.Context.Self.Path.Name
    let basePath = "akka://chord-system/user/supervisor/"
    let mutable state = "active"
    let mutable next = 1
    let nodeInt:bigint = convertToBigInt currNode


    let getActorRef id:bigint =
        // let hashID = id |> bigint |> ToString("x2")
        let hashID = id.ToString  "X"
        let path = basePath+hashID
        let actorRef = select path chord_system
        actorRef


    let get_next_node id = 
        let mutable index = hash_length-1
        while id < finger_table.[index] do
               index <- (index-1)
               //REMOVE
            //    printfn "table index %i for %s" index currNode
        
        finger_table.[index]

    let find_closest_preceeding_node id =
        let mutable index = hash_length-1
        //if we have to crossover
        if id < nodeInt then
            if finger_table.[index] > nodeInt then
                finger_table.[index]
                // printfn "index %i" index              
            else
                while id < finger_table.[index] && finger_table.[index-1]<= finger_table.[index] do 
                    index <- (index-1)
                    // printfn "index %i" index
                if finger_table.[index-1]> finger_table.[index] && id< finger_table.[index] then
                    index <- (index-1)
                    // printfn "index %i" index
                finger_table.[index]
        else
            //it has crossed over which we don't want
            if finger_table.[index] < nodeInt then
                while finger_table.[index-1] <= finger_table.[index] do
                    index <- (index-1)
                    printfn "index %i" index              
                index <- (index-1)
            while id < finger_table.[index] do
               index <- (index-1)
               printfn "index %i" index       
            finger_table.[index]
                
    
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Initialize(chord_nodes, current_index) -> 
            let total_nodes = chord_nodes.Length
            let current_node_int = chord_nodes.[current_index]
            predecessor <- chord_nodes.[(total_nodes + current_index - 1) % total_nodes].ToString("x2")
            if predecessor.Length = 40 then
                predecessor <- "0" + predecessor
            let chord_size =  bigint (2.0**160.0)
            for i in 1 .. 160 do
                let raw_value = current_node_int + bigint (2.0 ** float (i-1))
                let finger_int = raw_value % chord_size
                let mutable next_largest = bigint 0
                let offset = 0
                if finger_int > current_node_int then
                    next_largest <- chord_nodes.[binary_search chord_nodes.[(current_index+1)%total_nodes..] finger_int (current_index+1)%total_nodes]
                else
                    next_largest <- chord_nodes.[binary_search chord_nodes.[..current_index] finger_int 0]
                let mutable finger_val = next_largest.ToString("x2")
                if finger_val.Length = 40 then
                    finger_val <- "0" + finger_val
                finger_table.[i-1] <- finger_val
               
            mailbox.Sender() <! Init_Done

        | FindSuccessor(id, asker, purpose) ->             
            let successor = finger_table.[0]
            if (id > nodeInt && id <= successor) || (successor< nodeInt && (id> nodeInt || id<= successor)) then
                //means join
                if purpose = -1 then
                    let actorRef = getActorRef id 
                    actorRef <! ChangeSuccessor
                //means update finger table
                else 
                    let actorRef = getActorRef asker
                    actorRef <! UpdateFinger(id, purpose)                 
            else
                let cpn = find_closest_preceeding_node id
                let actorRef = getActorRef cpn
                actorRef <! FindSuccessor(id, asker, purpose)

        | ChangeSuccessor(successor) ->
            finger_table.[0] <- successor

        | Create ->
            printfn "Create called"
            predecessor <- -1
            finger_table.[0] <- nodeInt

        | Join(existingNode) ->
            printfn "Join Called"
            predecessor <- -1
            let actorRef = getActorRef(existingNode)
            actorRef <! FindSuccessor(nodeInt, nodeInt, -1)

        | TellYourPred ->
            mailbox.Sender <! Stabilize(predecessor)

        | Stabilize(x) ->
            if x <> -1 then
                // crossover
                if(finger_table.[0] < nodeInt) then
                    if x < finger_table.[0] || x > nodeInt then
                        finger_table.[0] <- x
                else if (x > nodeInt && x < finger_table.[0]) then
                    finger_table.[0] <- x
            let actorRef = getActorRef finger_table.[0]
            actorRef <! Notify(nodeInt)

        | Notify(pred) ->
            if (predecessor = -1) || (pred> nodeInt && (pred > predecessor || pred < selfName)) || (pred> predecessor && pred< nodeInt) then
                predecessor <- pred

        | TellState ->
            mailbox.Sender <! state
         
        | CheckPred(state) ->
            if state = "failed" then
                predecessor <- -1
            
        | FixFingers ->
            next <- next + 1
            if next >= hash_length then
                next <- 1             
            let power = (next-1) |>bigint
            let addition:bigint = nodeInt + bigint(2.0 ** power) 
            let id =  (addition % maxNode)
            mailbox.Self <! FindSuccessor(id, nodeInt, next)
        
        | UpdateFinger(id, index) ->
            finger_table.[index] <- id

        // | Begin_Simulation ->
        //     for i in 1 .. num_message do
        //         //generate random message and hash it
        //         let randomMsg = ranStr(50) 
        //         let hashedMsg = hash_string(randomMsg, hash_type)  
        //         //REMOVE
        //         //if key already in successor
        //         // if (currNode < predecessor && (hashedMsg > predecessor || hashedMsg <= currNode)) || (hashedMsg > predecessor && hashedMsg <= currNode) then
        //             //REMOVE
        //             // printfn "Converging message"
        //             // mailbox.Context.Parent <! Received_Message(0)
        //         // else
        //             //if successor has key
        //         let successor = finger_table.[0]
        //         if (hashedMsg > nodeInt && hashedMsg <= successor) || (successor< nodeInt && (hashedMsg> nodeInt || hashedMsg<= successor)) then
        //             let path = basePath+successor
        //             let actorRef = select path chord_system
        //             mailbox.Context.Parent <! Received_Message(1)
        //             //REMOVE 
        //             // printfn "routing %s to %s" hashedMsg path
        //             // actorRef <! Route(hashedMsg, 0) 
        //         else
        //             let next_node = find_closest_preceeding_node(hashedMsg)
        //             let mutable path = basePath+next_node 
        //             let actorRef = select path chord_system
                    
        //             actorRef <! Route(hashedMsg, 0)


        // | Route(hashedMsg, hop) ->
        //     let mutable num_hops = hop + 1
         
        //     let successor = finger_table.[0]
        //     if (hashedMsg > nodeInt && hashedMsg <= successor) || (successor< nodeInt && (hashedMsg> nodeInt || hashedMsg<= successor)) then
        //         let path = basePath+successor
        //         let actorRef = select path chord_system 
        //         mailbox.Context.Parent <! Received_Message(num_hops+1)

        //         //REMOVE
        //         // printfn "routing %s to SUCCESSOR %s" hashedMsg path
        //         // actorRef <! Route(hashedMsg, num_hops) 
        //     else
        //         let next_node = find_closest_preceeding_node(hashedMsg)
        //         let mutable path = basePath+next_node 
        //         let actorRef = select path chord_system
        //         //REMOVE
        //         // printfn "routing %s to %s" hashedMsg path
        //         // printfn "send key to next closest preceeding node %s %s" hashedMsg next_node
        //         actorRef <! Route(hashedMsg, num_hops)
                
        return! loop()
    }
    loop()

let Supervisor (mailbox : Actor<_>) =
    let mutable init_count = 0
    let mutable node_count = 0
    let mutable chord_list = new List<IActorRef>()
    let firstNode:bigint = 0 |>bigint
    let total_requests = num_nodes * num_message
    printfn "total requests = %i" total_requests
    let mutable average_hops = 0
    let mutable count = 0
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Start(nodes, messages) -> 
            let prefix = "chord_node_"
            for i in 1 .. nodes do
                let nodeName = ranStr 10
                let hashedValue = hash_string nodeName
                let intValue = convertToBigInt hashedValue
                let actorRef = spawn mailbox.Context hashedValue (actorOf2 Chord_Node)
                if i=1 then
                    firstNode <- intValue
                    actorRef <! Create
                else                                  
                    actorRef <! Join(firstNode)
                

        | Init_Done ->  init_count <- init_count + 1
                        if init_count = node_count then
                            printfn "Chord ring created"
                            chord_list |> Seq.iteri (fun i chord_node -> 
                                    chord_node <! Begin_Simulation)
                            // chord_list.[9] <! Begin_Simulation
        | Received_Message(hops) ->
            count <- count + 1
            average_hops <- average_hops + hops

            // REMOVE
            printfn "request converged %i in %i hops" count hops

            if count = total_requests then
                //calculate average
                average_hops <- average_hops / total_requests
                printfn "Average hops for %i nodes and %i requests per node = %i" num_nodes num_message average_hops
                mailbox.Context.System.Terminate() |> ignore             

        return! loop()
    }
    loop()

let supervisor = spawn chord_system "supervisor" Supervisor
supervisor <! Start(num_nodes, num_message)
chord_system.WhenTerminated.Wait()
