#time "on"

#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.FSharp
open System.Security.Cryptography

type Command =
| Start of zeros : int
| Continue

type InputObject =
    | MinerInput of zeros: int * length: int * gatorlink: string
    | PrinterInput of coin: string * hash: string

let mutable precedingZeroes: int  = 4
let actorCount = Environment.ProcessorCount

let listenerActor (mailbox: Actor<_>) message =
    printfn "%s" message
    match message with
    | "Available" -> mailbox.Sender () <! string precedingZeroes
    | _ -> ()

let printerActor message =
    printfn "%s" message

let bitcoinMinerActor (mailbox: Actor<_>) message =
    let ranStr n = 
        let r = Random()
        let chars = Array.concat([[|'a' .. 'z'|];[|'A' .. 'Z'|];[|'0' .. '9'|]])
        let sz = Array.length chars in
        String(Array.init n (fun _ -> chars.[r.Next sz])) 
        
    let generateAndValidateCoin (zeros:int, length: int, gatorlink: string) =
        let validate (zeros:int) (hashedStr:string) :  bool =
            hashedStr.StartsWith((String.replicate zeros "0"))

        let bitcoin: string  = gatorlink + (ranStr length)
        let hashBytes = bitcoin 
                            |> System.Text.Encoding.ASCII.GetBytes
                            |> (new SHA256Managed()).ComputeHash
        let hashString = hashBytes
                            |> Seq.map (fun c -> c.ToString "x2")
                            |> Seq.reduce (+)
        if validate zeros hashString then 
            select "/user/printerActor" mailbox.Context.System
                <! (bitcoin+"  "+hashString)

    match box message with
               | :? InputObject as inputObj ->
                   match inputObj with
                   | MinerInput (zeros, length, gatorlink) ->
                       generateAndValidateCoin(zeros, length, gatorlink)
                       mailbox.Self <! message
                   | _ -> ()
               | _ -> ()


let master (mailbox: Actor<_>) message =
    let mineCoins(input: int) =
        let minercount = actorCount
        let minerActorsList = [for a in 1l .. minercount do yield (spawn mailbox.Context ("bitcoinminer" + string a) (actorOf2 (bitcoinMinerActor)))]


        for a in 0l .. (minercount-1l) do
            minerActorsList.Item(a |> int) <! MinerInput (input, a+7, "lawande.s;")
            
           //let selection = select ("user/masterActor/remotebitcoinminer"+string a) mailbox.Context.System 

    match box message with
    | :? Command as command ->
        match command with
        | Start(zeros) -> 
            precedingZeroes <- zeros |> int
            mineCoins(precedingZeroes)
        | _ -> ()
    | _ -> ()

let config =
    Configuration.parse
        @"akka {
            actor{
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                    hostname = 192.168.0.184
                    port = 3536              
            }
        }"

match fsi.CommandLineArgs.Length with
| 2l -> precedingZeroes <- fsi.CommandLineArgs.[1] |> int
| _ -> ()
let myActorSystem = System.create "bitcoin-miner-server" config

let masterActorRef = spawn myActorSystem "masterActor" (actorOf2 master)
spawn myActorSystem "printerActor" (actorOf printerActor) |> ignore
spawn myActorSystem "listenerActor" (actorOf2 listenerActor) |> ignore

masterActorRef <! Start(precedingZeroes)

Console.ReadLine() |> ignore
