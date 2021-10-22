#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Text

let mutable numNodes = fsi.CommandLineArgs.[1] |> int64
let mutable ring = []
// let requests = fsi.CommandLineArgs.[2]
let system = ActorSystem.Create("Project3")

type Communication =
    | Start of string
    | BuildFingerTable of string
    | Initiate of String //list <IActorRef>

let nodes = numNodes |> float
let m = Math.Round(Math.Log(nodes, 2.)) |> int

Console.WriteLine(m)

let sha1Hash input: string =
    let sha = new SHA1Managed()
    let hashB = sha.ComputeHash(Encoding.ASCII.GetBytes(input.ToString()))

    let hashS =
        hashB
        |> Array.map (fun (x: byte) -> String.Format("{0:X2}", x))
        |> String.concat String.Empty
    hashS

let mutable seed = "Peer_"

for i in 1 .. 17 do
    seed <- seed + i.ToString()
    let ans = sha1Hash seed
    let position = ans.[0..m]
    let decValue = Convert.ToInt64(position, 16)
    Console.WriteLine(decValue)


let peer (mailbox: Actor<_>) =
    let mutable predecessor = null
    let mutable successor = null

    let rec loop() =
        actor {
            let! peermessage = mailbox.Receive()

            match peermessage with
            | Initiate(_) ->
                successor <- mailbox.Self
                Console.WriteLine("Ring created")
                let hashedValue = sha1Hash mailbox.Self
                Console.WriteLine("Hash: " + hashedValue)
                let position = hashedValue.[0..m - 1]
                let decValue = Convert.ToInt64(position, 16)
                let ringPosition = (decValue |> int) % pown 2 m
                ring <- ringPosition :: ring
                Console.WriteLine(ring)


            | _ -> ignore()

            return! loop()
        }

    loop()

let master (mailbox: Actor<_>) =
    let mutable peersList = []
    let numNodes = numNodes |> int

    let rec loop() =
        actor {
            let! message = mailbox.Receive()
            match message with
            | Start(_) ->
                peersList <-
                    [ for i in 1 .. numNodes do
                        yield (spawn system ("Peer_" + string (i))) peer ]
                Console.WriteLine(peersList.ToString())
                peersList.[0] <! Initiate("Begin")


            | _ -> ignore()

            return! loop()
        }
    loop()

let masterActor = spawn system "master" master

masterActor <! Start("Start")
