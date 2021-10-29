open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Collections.Generic

let num_nodes = 10
let num_message = 1
let hash_length = 160

type Supervisor_Messge = 
    | Start of nodes:int * messages:int
    | Init_Done

type Node_Message =
    | Initialize of chord_nodes: bigint [] * current_index: int
    | Begin_Simulation

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
    //if high = max_limit then
    //    offset
    //else
    high + offset

let Chord_Node (mailbox : Actor<_>) =
    let finger_table = Array2D.init hash_length 3 (fun _ _ -> "")
    let mutable previous_node = ""
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Initialize(chord_nodes, current_index) -> 
            let total_nodes = chord_nodes.Length
            let current_node_int = chord_nodes.[current_index]
            previous_node <- "0" + chord_nodes.[(total_nodes + current_index - 1) % total_nodes].ToString("x2")
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
                //let rec find_next_max offset =
                //    let cur_value = chord_nodes.[(current_index + offset) % total_nodes]
                //    if cur_value > finger_int then
                //        next_largest <- cur_value
                //    elif offset < total_nodes then
                //        find_next_max (offset+1)
                //find_next_max offset

                let mutable finger = finger_int.ToString("x2")
                if finger.Length = 40 then
                    finger <- "0" + finger
                let mutable finger_val = next_largest.ToString("x2")
                if finger_val.Length = 40 then
                    finger_val <- "0" + finger_val
                finger_table.[i-1, 0] <- finger
                finger_table.[i-1, 1] <- finger_val
                finger_table.[i-1, 2] <- ""
                //printfn "%O %O" finger finger_val
            mailbox.Sender() <! Init_Done

        | _ -> ()

        return! loop()
    }
    loop()

let Supervisor (mailbox : Actor<_>) =
    let mutable init_count = 0
    let mutable node_count = 0
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Start(nodes, messages) -> let prefix = "chord_node_"
                                    node_count <- nodes
                                    let node_ids = [for i in 1 .. nodes do yield hash_string(prefix+string(i), hash_type)] |> List.sort
                                    let node_ids_int = [|for i in node_ids -> bigint.Parse(i, System.Globalization.NumberStyles.HexNumber)|]
                                    let chord_list = new List<IActorRef>()
                                    node_ids |> List.iter (fun node_id -> chord_list.Add (spawn mailbox.Context node_id Chord_Node))
                                    for i in node_ids do
                                        printfn "%O %O" i (bigint.Parse(i, System.Globalization.NumberStyles.HexNumber) % bigint (2.0**160.0))
                                    chord_list |> Seq.iteri (fun i chord_node -> 
                                        chord_node <! Initialize(node_ids_int, i))
                                    //chord_list.[9] <! Initialize(node_ids_int, 9)

        | Init_Done ->  init_count <- init_count + 1
                        if init_count = node_count then

        return! loop()
    }
    loop()

[<EntryPoint>]
let main argv =
    let chord_system = System.create "chord-system" (Configuration.load())
    let supervisor = spawn chord_system "supervisor" Supervisor
    supervisor <! Start(num_nodes, num_message)
    let sample = "chord_node_"
    //printfn "%s" (hash_string(sample, hash_type))
    let chord_list = new List<string>()
    [1 .. 10] |> List.iter (fun i -> chord_list.Add <| hash_string(sample+string(i), hash_type))
    let zz = [for i in 1 .. 10 do yield hash_string(sample+string(i), hash_type)] |> List.sort
    chord_list.Sort()
    //for i in zz do
    //    printfn "%O %O" i (bigint.Parse(i, System.Globalization.NumberStyles.HexNumber) % bigint (2.0**160.0))
    Console.ReadLine() |> ignore
    0