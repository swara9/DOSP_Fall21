#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Collections.Generic

let num_nodes = 10
let num_message = 5
let hash_length = 160

let chord_system = System.create "chord-system" (Configuration.load())

type Supervisor_Messge = 
    | Start of nodes:int * messages:int
    | Init_Done
    | Received_Message of no_of_hops: int

type Node_Message =
    | Initialize of chord_nodes: bigint [] * current_index: int
    | Begin_Simulation
    | Route of message: string * hop: int

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


let binary_search (arr:bigint []) key offset =
    let max_limit = arr.Length
    let mutable low = 0
    let mutable high = arr.Length
    while low < high do
        let mid = low + (high-low)/2
        if arr.[int <| mid]<key then
            low <- mid + 1
        else
            high <- mid;    
    high + offset


let ranStr n = 
    let r = Random()
    let chars = Array.concat([[|'a' .. 'z'|];[|'A' .. 'Z'|];[|'0' .. '9'|]])
    let sz = Array.length chars in
    String(Array.init n (fun _ -> chars.[r.Next sz]))

let Chord_Node (mailbox : Actor<_>) =
    let mutable finger_table = Array2D.init hash_length 2 (fun _ _ -> "")
    // let mutable finger_table = new List<string>()
    let mutable predecessor = ""
    let selfName = mailbox.Context.Self.Path.Name

    let find_closest_preceeding_node id =
        let mutable index = hash_length-1
        if id < selfName then
            while finger_table.[index, 0] > id && index <> 0 do
                printfn "currIndex case 0 %i" index
                index <- (index-1)
            if index = 0 then
                finger_table.[(hash_length-1), 1]
            else
                finger_table.[index, 1]                 
        
            
        else if finger_table.[index, 1] < selfName then
            while finger_table.[index, 0]< id && index <> 0 do
                printfn "currIndex case 1 %i" index
                index <- (index - 1)
            if index <> 0 then
                while finger_table.[index, 0]> id do
                    printfn "currIndex case 2 %i" index
                    index <- (index - 1)
            else
                index <- (hash_length-1)
            finger_table.[index, 1]

        else
            while finger_table.[index, 0]> id do
                printfn "currIndex case 3 %i" index
                index <- (index - 1)
            finger_table.[index, 1]



        // if whole table traversed
        // if index = 0 then
        //     while id < finger_table.[index, 0] do
        //     index <- index - 1
        //     printfn "currIndex at last %i" index
        // finger_table.[index, 1]
                
    let basePath = "akka://chord-system/user/supervisor/"
    
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Initialize(chord_nodes, current_index) -> 
            let total_nodes = chord_nodes.Length
            let current_node_int = chord_nodes.[current_index]
            predecessor <- "0" + chord_nodes.[(total_nodes + current_index - 1) % total_nodes].ToString("x2")
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
                
                let mutable finger = finger_int.ToString("x2")
                if finger.Length = 40 then
                    finger <- "0" + finger
                let mutable finger_val = next_largest.ToString("x2")
                if finger_val.Length = 40 then
                    finger_val <- "0" + finger_val
                //let mutable path = "akka://chord-system/user/supervisor/"+finger_val
                finger_table.[i-1, 0] <- finger
                finger_table.[i-1, 1] <- finger_val
                // finger_table.Add(finger_val)
                // finger_table.[i-1, 2] <- ""

            mailbox.Sender() <! Init_Done

        | Begin_Simulation ->
            for i in 1 .. num_message do
                //generate random message and hash it
                let randomMsg = ranStr(50) 
                let hashedMsg = hash_string(randomMsg, hash_type)  
                //REMOVE
                //if key already in successor
                if (selfName < predecessor && (hashedMsg > predecessor || hashedMsg < selfName)) || (hashedMsg > predecessor && hashedMsg <= selfName) then
                    //REMOVE
                    // printfn "Converging message"
                    mailbox.Context.Parent <! Received_Message(0)
                else
                    //if successor has key
                    let successor = finger_table.[0,1]
                    if (hashedMsg > selfName && hashedMsg <= successor) || (successor< selfName && (hashedMsg> selfName || hashedMsg< successor)) then
                        let path = basePath+successor
                        let actorRef = select path chord_system 
                        actorRef <! Route(hashedMsg, 0) 
                    else
                        let next_node = find_closest_preceeding_node(hashedMsg)
                        let mutable path = basePath+next_node 
                        let actorRef = select path chord_system
                        // printfn "send key to next closest preceeding node %s %s" hashedMsg next_node
                        actorRef <! Route(hashedMsg, 0)


        | Route(hashedMsg, hop) ->
            let mutable num_hops = hop + 1
            //if message lesser than or equal to successor
            if (selfName < predecessor && (hashedMsg > predecessor || hashedMsg < selfName))  || (hashedMsg > predecessor && hashedMsg <= selfName) then
                //REMOVE
                // printfn "Converging message"
                mailbox.Context.Parent <! Received_Message(num_hops)
            else
                //if successor has key
                let successor = finger_table.[0, 1]
                if (hashedMsg > selfName && hashedMsg <= successor) || (successor< selfName && (hashedMsg> selfName || hashedMsg< successor)) then
                    let path = basePath+successor
                    let actorRef = select path chord_system 
                    actorRef <! Route(hashedMsg, num_hops) 
                else
                    let next_node = find_closest_preceeding_node(hashedMsg)
                    let mutable path = basePath+next_node 
                    let actorRef = select path chord_system
                    // printfn "send key to next closest preceeding node %s %s" hashedMsg next_node
                    actorRef <! Route(hashedMsg, num_hops)
                
        return! loop()
    }
    loop()

let Supervisor (mailbox : Actor<_>) =
    let mutable init_count = 0
    let mutable node_count = 0
    let mutable chord_list = new List<IActorRef>()

    let total_requests = num_nodes * num_message
    printfn "total requests = %i" total_requests
    let mutable average_hops = 0
    let mutable count = 0
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Start(nodes, messages) -> let prefix = "chord_node_"
                                    node_count <- nodes
                                    let node_ids = [for i in 1 .. nodes do yield hash_string(prefix+string(i), hash_type)] |> List.sort
                                    let node_ids_int = [|for i in node_ids -> bigint.Parse(i, System.Globalization.NumberStyles.HexNumber)|]
                                    // chord_list = new List<IActorRef>()
                                    node_ids |> List.iter (fun node_id -> chord_list.Add (spawn mailbox.Context node_id Chord_Node))
                                    // for i in node_ids do
                                        // printfn "%O %O" i (bigint.Parse(i, System.Globalization.NumberStyles.HexNumber) % bigint (2.0**160.0))
                                    chord_list |> Seq.iteri (fun i chord_node -> 
                                         chord_node <! Initialize(node_ids_int, i))
                                    //chord_list.[9] <! Initialize(node_ids_int, 9)

        | Init_Done ->  init_count <- init_count + 1
                        if init_count = node_count then
                            printfn "Chord ring created"
                            chord_list |> Seq.iteri (fun i chord_node -> 
                                    chord_node <! Begin_Simulation)
                            // chord_list.[9] <! Begin_Simulation
                            // chord_list.[6] <! Begin_Simulation


        | Received_Message(hops) ->
            count <- count + 1
            average_hops <- average_hops + hops

            //REMOVE
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
// let sample = "chord_node_"
//printfn "%s" (hash_string(sample, hash_type))
// let chord_list = new List<string>()
// [1 .. 10] |> List.iter (fun i -> chord_list.Add <| hash_string(sample+string(i), hash_type))
// let zz = [for i in 1 .. 10 do yield hash_string(sample+string(i), hash_type)] |> List.sort
// chord_list.Sort()
//for i in zz do
//    printfn "%O %O" i (bigint.Parse(i, System.Globalization.NumberStyles.HexNumber) % bigint (2.0**160.0))
// Console.ReadLine() |> ignore
