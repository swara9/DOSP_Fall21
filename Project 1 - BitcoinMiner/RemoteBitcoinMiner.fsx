#time "on"

#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.FSharp
open System.Security.Cryptography

let mutable serverIP = "localhost"
let actorCount = Environment.ProcessorCount
let workUnit = 1000000

type InputObject =
| MinerInput of zeros: int * length: int * gatorlink: string

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
            let selection = mailbox.Context.System.ActorSelection("akka.tcp://bitcoin-miner-server@" + serverIP + ":3536/user/printerActor")
            selection <! (bitcoin + "   " + hashString)
        
    match box message with                   
               | :? InputObject as inputObj ->
                   match inputObj with
                   | MinerInput (zeros, length, gatorlink) ->
                       generateAndValidateCoin(zeros, length, gatorlink)
                       mailbox.Self <! message
               | _ -> ()

let remoteMasterActor (mailbox: Actor<_>) message =
    let mineCoins input =
        let minercount = actorCount
        let minerActorsList = [for a in 1l .. minercount do yield (spawn mailbox.Context ("remotebitcoinminer" + string a) (actorOf2 (bitcoinMinerActor)))]
                    
        for a in 0l .. (minercount-1l) do
            minerActorsList.Item(a |> int) <! MinerInput (input, a+7, "vikashpakasipand;")

    match message with
    | "start"
        -> mailbox.Context.System.ActorSelection("akka.tcp://bitcoin-miner-server@"+serverIP+":3536/user/listenerActor") <! "Available"
    | _ -> mineCoins( message |> int)

let config =
    Configuration.parse
        @"akka {
            actor{
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                    hostname = localhost
                    port = 0               
            }
        }"


match fsi.CommandLineArgs.Length with
| 2l -> serverIP <- fsi.CommandLineArgs.[1]
| _ -> ()

let system = System.create "remote-system" config

let remoteMaster = spawn system "remoteMasterActor" (actorOf2 remoteMasterActor)
remoteMaster <! "start"
Console.ReadLine() |> ignore

