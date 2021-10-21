#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Text

// let mutable numNodes = fsi.CommandLineArgs.[1] |> int
// let requests = fsi.CommandLineArgs.[2]
// let system = ActorSystem.Create("Project3")
type Communication = 
    | Start of string
    | BuildFingerTable of string


let sha1Hash input : string=
    let sha = new SHA1Managed()
    let hashB = sha.ComputeHash(Encoding.ASCII.GetBytes(input.ToString()))
    let hashS =
        hashB
        |> Array.map (fun (x: byte) -> String.Format("{0:X2}", x))
        |> String.concat String.Empty
    hashS
let mutable seed = "Peer_" 
for i in 1..300 do 
    seed <- seed + i.ToString()
    let ans = sha1Hash seed   
    let position = ans.[ans.Length - 8 .. ans.Length]
    let decValue = Convert.ToInt32(position, 16);
    Console.WriteLine(ans.Length)

// let peer (mailbox: Actor<_>) =
//     let mutable predecessor = null
//     let mutable successor = null
//     let rec loop () =
//         actor {
//             let! peermessage = mailbox.Receive()
//             match peermessage with
//             | BuildFingerTable(_) -> 

//         }
//         loop()

// let master (mailbox: Actor<_>) =
//     let mutable peersList = []
//     let rec loop() = actor {
//             let! message = mailbox.Receive()
//             match message with
//             | Start (_) -> 
//                            peersList <-  [ for i in 1 .. numNodes do yield (spawn system ("Peer_" + string (i))) peer]
//                            Console.WriteLine(peersList.ToString())

//             | _ -> ignore()

//         // return! loop()
//     }
//     loop()

// let masterActor = spawn system "master" master
// masterActor <! Start("Start")