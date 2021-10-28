#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open System.Collections.Generic
open System.Collections.Specialized;
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Text

let mutable numNodes = fsi.CommandLineArgs.[1] |> int64
// let requests = fsi.CommandLineArgs.[2]
let system = ActorSystem.Create("Project3")

type Communication =
    | Start of string
    | BuildFingerTable of string
    | Initiate of String //list <IActorRef>
    | FindSuccessor of IActorRef
    | SetSuccessor of IActorRef
    | SetPredecessor of IActorRef
    | Temp of string * IActorRef
    | StaticInitiate of list<IActorRef>

// let nodes = numNodes |> float
let mutable m = 0//Math.Ceiling(Math.Log(nodes, 2.)) |> int

// let mutable ring = []
// let dummy = spawn system "dummy"
// let mutable ring = Array.create (pown 2 m) null

// Console.WriteLine(m)

let sha1Hash input: string =
    let sha = new SHA1Managed()
    let hashB = sha.ComputeHash(Encoding.ASCII.GetBytes(input.ToString()))

    let hashS =
        hashB
        |> Array.map (fun (x: byte) -> String.Format("{0:X2}", x))
        |> String.concat String.Empty
    hashS

// let mutable seed = "Peer_"

// for i in 1 .. 17 do
//     seed <- seed + i.ToString()
//     let ans = sha1Hash seed
//     let position = ans.[0..m]
//     let decValue = Convert.ToInt64(position, 16)
//     Console.WriteLine(decValue)
// let buildFingerTable input: string =

        
let peer (mailbox: Actor<_>) =
    let mutable fingerTable = []//OrderedDictionary()
    let mutable predecessor = null
    let mutable successor = null
    let mutable selfAddress = 0

    let rec loop() =
        actor {
            let! peermessage = mailbox.Receive()

            match peermessage with
            | Initiate(_) ->
                successor <- mailbox.Self
                selfAddress <- mailbox.Self.Path.Name.Split('_').[1] |> int
                //fingerTable.Add(selfAddress + 1, successor)
                fingerTable <- List.append fingerTable [successor]
                //Console.WriteLine(fingerTable.[0])
                // Console.WriteLine("Ring created")
                // let hashedValue = sha1Hash mailbox.Self.Path.Name.Split("_")
                // Console.WriteLine("Hash: " + hashedValue)
                // let position = hashedValue.[hashedValue.Length - m.. 40]
                // Console.WriteLine("Position: " + position)
                // let decValue = Convert.ToInt64(position, 16)
                // Console.WriteLine("Dec value: " + decValue.ToString())
                // let temp = decValue |> uint
                // Console.WriteLine("Debug" + temp.ToString())
                // let a = 2 |> uint
                // let b = pown a m |> uint
                // let ringPosition = (decValue |> uint) % b |> int
                // ring <- ringPosition :: ring
                // ring.[ringPosition] <- mailbox.Self.Path.Name
                // mailbox.Sender() <! Temp(hashedValue, mailbox.Self)
                // Array.set ring ringPosition mailbox.Self
                // Console.WriteLine("Ring " + (Array.get ring ringPosition).ToString())


            | FindSuccessor(nodeRef) ->
                // Console.WriteLine(mailbox.Self)
                // Console.WriteLine(nodeRef)
                let numId = nodeRef.Path.Name.Split('_').[1] |> int
                let succId = successor.Path.Name.Split('_').[1] |> int
                //let mutable break
                if numId > selfAddress && numId < succId then
                    nodeRef <! SetSuccessor(successor)
                    nodeRef <! SetPredecessor(mailbox.Self)
                    //nodeRef <! SetSuccessor(successor)
                else
                    let mutable tempBreak = false
                    let mutable i = m-1
                    let mutable fingerId = 0
                    while not tempBreak && i >= 0 do 
                        fingerId <- fingerTable.[i].Path.Name.Split('_').[1] |> int
                        if fingerId > selfAddress && fingerId < numId then
                            tempBreak <- true
                        else
                            i <- i - 1
                    if tempBreak then 
                        fingerTable.[i] <! FindSuccessor(nodeRef)
                    else
                        nodeRef <! SetSuccessor(successor)
                        nodeRef <! SetPredecessor(mailbox.Self)

            
            | StaticInitiate(initialList) ->
                let mutable list = []
                let mutable temp = 0
                let mutable a = []
                for i in 1..m do
                    temp <- (selfAddress + pown 2 i-1) % initialList.Length
                    a <- initialList |> List.indexed |> List.filter(fun(_,x)-> x.Path.Name.Split('_').[1] |> int = temp) |> List.map fst
                    while List.isEmpty a do
                        temp <- temp + 1
                        a <- initialList |> List.indexed |> List.filter(fun(_,x)-> x.Path.Name.Split('_').[1] |> int = temp) |> List.map fst
                    // Console.WriteLine a
                    list <- List.append list [initialList.[a.[0]]] 
                fingerTable <- list
                successor <- fingerTable.[0]
                //Console.WriteLine ("Node " + selfAddress.ToString() + " " + fingerTable.ToString())
                // if selfAddress = 1 then
                //     Console.WriteLine successor
 


            | SetSuccessor(nodeRef) ->
                successor <- nodeRef
                // fingerTable.[selfAddress + 1] <- successor
                fingerTable <- List.append fingerTable [successor]
                Console.WriteLine ("New Node" + mailbox.Self.ToString())
                Console.WriteLine ("successor" + successor.ToString())  
                              
            | SetPredecessor(nodeRef) ->
                predecessor <- nodeRef
                Console.WriteLine ("Node" + mailbox.Self.ToString())
                Console.WriteLine ("Predecessor" + predecessor.ToString())                

            | _ -> ignore()

            return! loop()
        }

    loop()

let master (mailbox: Actor<_>) =
    let mutable peersList = []
    let numNodes = numNodes |> int
    // let mutable ring = Array.create (pown 2 m) null
    let rec loop() =
        actor {
            let! message = mailbox.Receive()
            match message with
            | Start(_) ->
                peersList <-
                    [ for i in 0 .. numNodes-1 do
                        yield (spawn system ("Peer_" + string (i))) peer ]
                
                // Console.WriteLine(peersList.ToString())
                // peersList.[0] <! Initiate("Begin")
                let initialList = List.append peersList.[0..2] peersList.[90..100]
                m <- Math.Ceiling(Math.Log(numNodes |> float, 2.)) |> int
                Console.WriteLine ("m " + m.ToString())
                // for i in initialList do
                //     Console.WriteLine(i)
                initialList
                |> List.iter (fun node ->
                        node
                        <! Initiate("Begin"))

                initialList
                |> List.iter (fun node ->
                        node
                        <! StaticInitiate(initialList))         

                let rnd = Random()
                let init = rnd.Next(0,initialList.Length)
                let mutable fin = 0
                // while fin = init do
                fin <- rnd.Next(5,peersList.Length)

                Console.WriteLine ("Init " + init.ToString())
                Console.WriteLine ("Fin " + fin.ToString())

                initialList.[1] <! FindSuccessor(peersList.[50])    


            | Temp(hashedValue, selfAddress) -> Console.WriteLine("Hashed Value: " + hashedValue)
                                                // Array.set ring hashedValue selfAddress
                                                //  Console.WriteLine("Position " + ringPosition.ToString() + " " + (Array.get ring ringPosition).ToString())
                                                //  for y in ring do
                                                //     Console.WriteLine(Array.get ring y)

            | _ -> ignore()

            return! loop()
        }
    loop()

let masterActor = spawn system "master" master

masterActor <! Start("Start")
system.WhenTerminated.Wait()
