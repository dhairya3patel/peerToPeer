#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Security.Cryptography
open System.Text

let mutable numNodes = fsi.CommandLineArgs.[1] |> int
let numRequests = fsi.CommandLineArgs.[2] |> int

let config =
    ConfigurationFactory.ParseString(
        @"akka {
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }"
    )

let system = ActorSystem.Create("Project3", config)

type Communication =
    | Start of string
    | BuildFingerTable of string
    | Initiate of String //list <IActorRef>
    | Join of String
    | FindSuccessor of IActorRef * list<IActorRef>
    | SetSuccessor of IActorRef * list<IActorRef>
    | SetPredecessor of IActorRef
    | MyPredecessor of IActorRef
    | RevertPredecessor of IActorRef * list<IActorRef>
    | Stabilize of list<IActorRef>
    | StabilizeReceiver of IActorRef * list<IActorRef>
    | Notify of IActorRef * IActorRef
    | Temp of string * IActorRef
    | StaticInitiate of list<IActorRef>
    | Lookup of String * String
    | LookupDone of String
    | Forward of IActorRef * String
    | StoreKey of String
    | SendLookup of String
    | DistributeKeys of IActorRef
    | ReceiveKeys of list<String>

let mutable m = 0
let rnd = Random()
let mutable keyList = []


let sha1Hash input : string =
    let sha = new SHA1Managed()

    let hashB =
        sha.ComputeHash(Encoding.ASCII.GetBytes(input.ToString()))

    let hashS =
        hashB
        |> Array.map (fun (x: byte) -> String.Format("{0:X2}", x))
        |> String.concat String.Empty

    hashS



let peer (mailbox: Actor<_>) =
    let mutable fingerTable = []
    let mutable predecessor = null
    let mutable successor = null
    let mutable selfAddress = 0
    let mutable selfHash = ""
    let mutable supervisorRef = mailbox.Self
    let mutable keys = []
    let mutable lookupCount = 0
    let mutable flag = false

    let buildFingerTable (ind: int) (currentList: list<IActorRef>) =
        let mutable list = []
        let mutable temp = 0
        let mutable a = []

        for i in ind..m do
            temp <- (selfAddress + pown 2 (i - 1))

            if temp > currentList.Length then
                temp <- temp % currentList.Length

            a <-
                currentList
                |> List.indexed
                |> List.filter (fun (_, x) -> x.Path.Name.Split('_').[1] |> int = temp)
                |> List.map fst

            while List.isEmpty a do
                temp <- temp + 1

                a <-
                    currentList
                    |> List.indexed
                    |> List.filter (fun (_, x) -> x.Path.Name.Split('_').[1] |> int = temp)
                    |> List.map fst

            let currentHash =
                currentList.[a.[0]].Path.Name.Split('_').[1]
                |> int

            list <- List.append list [ (currentList.[a.[0]], sha1Hash currentHash) ]

        list

    let rec loop () =
        actor {
            let! peermessage = mailbox.Receive()

            match peermessage with
            | Initiate (_) ->
                successor <- mailbox.Self

                selfAddress <- mailbox.Self.Path.Name.Split('_').[1] |> int
                selfHash <- sha1Hash selfAddress // mailbox.Self.Path.Name

                fingerTable <- List.append fingerTable [ successor, selfHash ]
                supervisorRef <- mailbox.Sender()


            | FindSuccessor (nodeRef, initialList) ->

                let numId = nodeRef.Path.Name.Split('_').[1] |> int
                let numHash = sha1Hash numId

                let succId =
                    successor.Path.Name.Split('_').[1] |> int

                let succHash = sha1Hash succId

                if numHash > selfHash && numHash < succHash then

                    nodeRef
                    <! SetSuccessor(fst (fingerTable.[0]), initialList)

                    nodeRef <! SetPredecessor(mailbox.Self)

                else

                    let mutable tempBreak = false
                    let mutable i = m - 1
                    let mutable fingerId = null

                    if fingerTable.Length = m then
                        while not tempBreak && i >= 0 do
                            fingerId <- snd (fingerTable.[i]) // fingerId <- (fst(fingerTable.[i]).Path.Name.Split('_').[1]) |> int

                            if fingerId > selfHash && fingerId < numHash then // fingerId > selfAddress && fingerId < numId then
                                tempBreak <- true
                            else
                                i <- i - 1

                        if tempBreak then
                            fst (fingerTable.[i])
                            <! FindSuccessor(nodeRef, initialList)
                        else
                            nodeRef
                            <! SetSuccessor(fst (fingerTable.[0]), initialList)


            | StaticInitiate (initialList) ->

                fingerTable <- buildFingerTable 1 initialList
                successor <- fst (fingerTable.[0])
                successor <! SetPredecessor(mailbox.Self)



            | SetSuccessor (nodeRef, initialList) ->

                successor <- nodeRef

                let succId =
                    successor.Path.Name.Split('_').[1] |> int

                let list = buildFingerTable 1 initialList
                fingerTable <- List.append [ (successor, sha1Hash succId) ] list.[1 .. list.Length - 1]

                nodeRef <! SetPredecessor(mailbox.Self)

                system.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.FromSeconds(3.0),
                    TimeSpan.FromSeconds(3.0),
                    nodeRef,
                    DistributeKeys(mailbox.Self)
                )


            | SetPredecessor (nodeRef) ->

                predecessor <- nodeRef

            | Stabilize (initialList) ->

                successor
                <! RevertPredecessor(mailbox.Self, initialList)

            | RevertPredecessor (nodeRef, initialList) ->

                mailbox.Sender()
                <! StabilizeReceiver(predecessor, initialList)



            | StabilizeReceiver (nodeRef, initialList) ->

                let x = nodeRef.Path.Name.Split('_').[1] |> int

                let succId =
                    successor.Path.Name.Split('_').[1] |> int

                if (selfAddress > succId && x > selfAddress)
                   || (selfAddress < succId
                       && x > selfAddress
                       && x < succId) then
                    mailbox.Self <! SetSuccessor(nodeRef, initialList)


            | Notify (self, nodeRef) ->

                let selfId = self.Path.Name.Split('_').[1] |> int
                let nodeRefId = nodeRef.Path.Name.Split('_').[1] |> int

                let predId =
                    predecessor.Path.Name.Split('_').[1] |> int

                if isNull predecessor
                   || (nodeRefId > predId && nodeRefId < selfId) then
                    self <! SetPredecessor(nodeRef)


            | Lookup (keyHash, msg) ->

                selfAddress <- mailbox.Self.Path.Name.Split("_").[1] |> int
                if keyHash > selfHash
                   && keyHash < snd (fingerTable.[0]) then

                    if msg = "Store" then
                        keys <- List.append keys [ keyHash ]
                        keyList <- List.append keyList [ keyHash ]
                    else
                        supervisorRef <! LookupDone("Done")

                else
                    let mutable low = ""
                    let mutable high = ""
                    let mutable tempBreak = false
                    let mutable i = 0

                    while not tempBreak && i < m - 1 do
                        low <- snd (fingerTable.[i])
                        high <- snd (fingerTable.[i + 1])

                        if keyHash > low && keyHash < high then

                            if msg = "Store" then
                                fst (fingerTable.[i]) <! Lookup(keyHash, "Store")
                            else
                                supervisorRef
                                <! Forward(fst (fingerTable.[i]), keyHash)

                            tempBreak <- true
                        else
                            i <- i + 1

                    if not tempBreak then
                        if msg = "Store" then
                            if selfAddress = numNodes then
                                successor <! StoreKey(keyHash)
                            else
                                fst (fingerTable.[m - 1])
                                <! Lookup(keyHash, "Store")
                        else
                            supervisorRef
                            <! Forward(fst (fingerTable.[m - 1]), keyHash)

            | StoreKey (keyHash) ->
                keys <- List.append keys [ keyHash ]
                keyList <- List.append keyList [ keyHash ]


            | DistributeKeys (nodeRef) ->
                let mutable sendKeys = []

                let predecessorHash =
                    sha1Hash (nodeRef.Path.Name.Split("_").[1])

                for currentKey in keys do
                    if currentKey < predecessorHash then

                        sendKeys <- List.append [ currentKey ] sendKeys

                        keys <-
                            keys
                            |> List.indexed
                            |> List.filter (fun (_, x) -> x <> currentKey)
                            |> List.map snd

                nodeRef <! ReceiveKeys(sendKeys)

            | ReceiveKeys (sendKeys) ->

                keys <- List.append sendKeys keys

            | SendLookup (_) ->

                let mutable key =
                    keyList.[rnd.Next(0, keyList.Length - 1)]

                while List.contains key keys do

                    key <- keyList.[rnd.Next(0, keyList.Length - 1)]

                if lookupCount < numRequests then

                    let mutable tempBreak = false
                    let mutable i = 0
                    let mutable low = null
                    let mutable high = null

                    while i < m - 1 && not tempBreak do

                        low <- snd (fingerTable.[i])
                        high <- snd (fingerTable.[i + 1])

                        if key > low && key < high then

                            tempBreak <- true
                            lookupCount <- lookupCount + 1
                            fst (fingerTable.[i]) <! Lookup(key, "Lookup")

                        else

                            i <- i + 1

            | _ -> ignore ()

            return! loop ()
        }

    loop ()

let master (mailbox: Actor<_>) =
    let mutable peersList = []
    let mutable initialList = []
    let numNodes = numNodes |> int
    let mutable hops = 0.0
    let mutable lookups = 0.0
    let mutable totalHops = 0.0

    let rec loop () =

        actor {

            let! message = mailbox.Receive()

            match message with
            | Start (_) ->
                peersList <-
                    [ for i in 1..numNodes do
                          yield (spawn system ("Peer_" + string (i))) peer ]

                let mutable tempList = []
                let rnd = Random()

                initialList <- peersList.[0..4]

                m <-
                    Math.Ceiling(Math.Log(initialList.Length |> float, 2.))
                    |> int

                peersList
                |> List.iter (fun node -> node <! Initiate("Begin"))

                initialList
                |> List.iter (fun node -> node <! StaticInitiate(initialList))

                let mutable init = null
                let mutable newNode = null

                for i in 5 .. numNodes - 1 do
                    init <- peersList.[rnd.Next(0, numNodes - 1)]
                    newNode <- peersList.[i]

                    system.Scheduler.ScheduleTellOnce(
                        TimeSpan.FromSeconds(1.0),
                        init,
                        FindSuccessor(newNode, initialList)
                    )

                    system.Scheduler.ScheduleTellOnce(
                        TimeSpan.FromSeconds(4.0),
                        peersList.[i - 1],
                        Stabilize(initialList)
                    )

                    initialList <- List.append initialList [ newNode ]
                    tempList <- List.append tempList [ newNode ]

                    m <-
                        Math.Ceiling(Math.Log(initialList.Length |> float, 2.))
                        |> int

                initialList
                |> List.iter (fun node ->
                    system.Scheduler.ScheduleTellRepeatedly(
                        TimeSpan.FromSeconds(5.0),
                        TimeSpan.FromSeconds(3.0),
                        node,
                        Stabilize(initialList)
                    ))

                initialList
                |> List.iter (fun node ->
                    system.Scheduler.ScheduleTellRepeatedly(
                        TimeSpan.FromSeconds(2.0),
                        TimeSpan.FromSeconds(1.0),
                        node,
                        StaticInitiate(initialList)
                    ))

                let mutable key = ""

                for i in 1 .. 2 * numNodes do

                    key <- sha1Hash (i.ToString())

                    system.Scheduler.ScheduleTellOnce(
                        TimeSpan.FromSeconds(10.0),
                        initialList.[rnd.Next(0, initialList.Length - 1)],
                        Lookup(key, "Store")
                    )

                initialList
                |> List.iter (fun node ->
                    system.Scheduler.ScheduleTellRepeatedly(
                        TimeSpan.FromSeconds(12.0),
                        TimeSpan.FromSeconds(1.0),
                        node,
                        SendLookup("Lookup")
                    ))


            | LookupDone (_) ->

                totalHops <- totalHops + 1.0
                hops <- hops + 1.0
                lookups <- lookups + 1.0

                Console.WriteLine(lookups.ToString() + " " + hops.ToString())

                hops <- 0.0

                if lookups = (numNodes * numRequests |> float) then

                    Console.WriteLine("Total Hops " + totalHops.ToString())
                    Console.WriteLine("Total Requests " + numRequests.ToString())
                    Console.WriteLine("Total Lookups " + lookups.ToString())

                    Console.WriteLine(
                        "Average Hops per lookup "
                        + (totalHops / lookups).ToString()
                    )

                    system.Terminate() |> ignore

            | Forward (dest, nodeRef) ->

                totalHops <- totalHops + 1.0
                hops <- hops + 1.0

                dest <! Lookup(nodeRef, "Lookup")

            | _ -> ignore ()

            return! loop ()
        }

    loop ()

let masterActor = spawn system "master" master

masterActor <! Start("Start")

system.WhenTerminated.Wait()
