#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open System.Collections.Generic
open System.Collections.Specialized
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
        }")

let system = ActorSystem.Create("Project3",config)
let mutable failedList = []
for i in numNodes/5 .. numNodes/5 + 2 do
    failedList <- List.append failedList [i]

// Console.WriteLine("Failed List: " + failedList.ToString())

type Communication =
    | Start of string
    | BuildFingerTable of string
    | Initiate of String //list <IActorRef>
    | Join of String
    | FindSuccessor of IActorRef*list<IActorRef>
    | SetSuccessor of IActorRef*list<IActorRef>
    | SetPredecessor of IActorRef
    | MyPredecessor of IActorRef
    | RevertPredecessor of IActorRef * list<IActorRef>
    | Stabilize of list<IActorRef>
    | StabilizeReceiver of IActorRef* list<IActorRef>
    | Notify of IActorRef * IActorRef
    | Temp of string * IActorRef
    | StaticInitiate of list<IActorRef>
    | Lookup of String*String
    | LookupDone of String
    | Forward of IActorRef*String
    | StoreKey of String
    | SendLookup of list<String>
    | DistributeKeys of IActorRef
    | ReceiveKeys of list<String>
    | Ping of String
    | PingResponse of String

// let nodes = numNodes |> float
let mutable m = 0//Math.Ceiling(Math.Log(nodes, 2.)) |> int
let rnd = Random()

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
    let mutable selfHash = ""
    let mutable supervisorRef = mailbox.Self
    let mutable keys = []
    let mutable lookupCount = 0
    let mutable flag = false

    let buildFingerTable (ind : int)(currentList:list<IActorRef>) = 
        let mutable list = []
        let mutable temp = 0
        
        let mutable a = []
        if not (List.contains selfAddress failedList ) then
            for i in ind..m do
                temp <- (selfAddress + pown 2 (i-1))
                if temp > (currentList.Length) then
                    temp <- temp % (currentList.Length)

                while List.contains temp failedList do
                    temp <- temp + 1
                // Console.WriteLine("debug " + selfAddress.ToString() + " " + temp.ToString())
                a <- currentList |> List.indexed |> List.filter(fun(_,x)-> x.Path.Name.Split('_').[1] |> int = temp) |> List.map fst
                // Console.WriteLine("Self Address: " + selfAddress.ToString()+ "a: " + a.ToString())
                while List.isEmpty a  do
                    temp <- temp + 1
                    a <- currentList |> List.indexed |> List.filter(fun(_,x)-> x.Path.Name.Split('_').[1] |> int = temp) |> List.map fst
                // Console.WriteLine a
                let currentHash = currentList.[a.[0]].Path.Name.Split('_').[1] |> int
                list <- List.append list [(currentList.[a.[0]] , sha1Hash currentHash)]
                // Console.WriteLine("Self: "+ selfAddress.ToString() + "List: " + list.ToString())
                
                    
            // Console.WriteLine("Dead Node")
        list

    let rec loop() =
        actor {
            let! peermessage = mailbox.Receive()

            match peermessage with
            | Initiate(_) ->
                successor <- mailbox.Self
                // successorAddress <- 
                selfAddress <- mailbox.Self.Path.Name.Split('_').[1] |> int
                selfHash <- sha1Hash selfAddress  // mailbox.Self.Path.Name
                //fingerTable.Add(selfAddress + 1, successor)
                fingerTable <- List.append fingerTable [successor, selfHash]
                supervisorRef <- mailbox.Sender()
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


            | FindSuccessor(nodeRef,initialList) ->
                // Console.WriteLine("Init " + mailbox.Self.ToString())
                // Console.WriteLine(mailbox.Self.ToString() + " " + fingerTable.ToString())
                
                let numId = nodeRef.Path.Name.Split('_').[1] |> int
                let succId = successor.Path.Name.Split('_').[1] |> int
                //Console.WriteLine(succId)
                //let mutable break
                if numId > selfAddress && numId < succId then
                    nodeRef <! SetSuccessor(fst(fingerTable.[0]),initialList)
                    nodeRef <! SetPredecessor(mailbox.Self)
                    // fst(fingerTable.[0]) <! Ping("Ping")
                    //nodeRef <! SetSuccessor(successor)
                else
                    let mutable tempBreak = false
                    let mutable i = m - 1
                    let mutable fingerId = 0
                    if fingerTable.Length = m then
                        while not tempBreak && i >= 0 do 
                            fingerId <- (fst(fingerTable.[i]).Path.Name.Split('_').[1]) |> int
                            if fingerId > selfAddress && fingerId < numId then
                                tempBreak <- true
                            else
                                i <- i - 1
                        if tempBreak then 
                            fst(fingerTable.[i]) <! FindSuccessor(nodeRef,initialList)
                        else
                            nodeRef <! SetSuccessor(fst(fingerTable.[0]),initialList)
                            // fst(fingerTable.[0]) <! Ping("Ping")
                            // successor <! SetPredecessor(mailbox.Self)
                            // Console.WriteLine("New Predecessor")
            
            | StaticInitiate(initialList) ->
                if not (List.contains (mailbox.Self.Path.Name.Split('_').[1] |> int) failedList ) then
                    fingerTable <- buildFingerTable 1 initialList
                    successor <- fst(fingerTable.[0])
                    successor <! SetPredecessor (mailbox.Self)
                    // let res = mailbox.Context.Watch(successor)  
                    Console.WriteLine(mailbox.Self.ToString() + "FT" + fingerTable.ToString()) 
                // else
                    // Console.WriteLine("Failed Node")        
                // fingerTable
                // |> List.iter (fun (node,_) ->
                //         // system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0),TimeSpan.FromSeconds(1.0),node ,Ping("Ping")))
                //     node <! Ping("Ping")) 
                    // mailbox.Context.Watch(system.ActorSelection(node.Path.Name)))
                    // system.Scheduler.ScheduleTell(TimeSpan.FromSeconds(1.0),TimeSpan.FromSeconds(1.0), node , Ping("Ping")))


                    // initialList.[r.Next(0,initialList.Length - 1)] <! Lookup(sha1Hash (r.Next(0,(numNodes |> int)).ToString()),"Lookup")

                // Console.WriteLine ("Node " + selfAddress.ToString() + " " + fingerTable.ToString())
                // if selfAddress = 1 then
                //     Console.WriteLine successor
 
            | SetSuccessor(nodeRef,initialList) ->
                successor <- nodeRef
                // Console.WriteLine("DEBUG Self Node: " + mailbox.Self.Path.Name + "SUCC " + successor.ToString())
                // Console.WriteLine("DEBUG Self Node: " + mailbox.Self.Path.Name + "PRED " + predecessor.ToString())
                // fingerTable.[selfAddress + 1] <- successor
                let succId = successor.Path.Name.Split('_').[1] |> int
                let list = buildFingerTable 1 initialList
                fingerTable <- List.append [(successor ,sha1Hash succId)] list.[1..list.Length - 1]
                
                // Console.WriteLine ("New Node" + mailbox.Self.ToString())
                // Console.WriteLine ("successor" + successor.ToString())  
                // Console.WriteLine("New node fingertable: " + fingerTable.ToString())
                nodeRef <! SetPredecessor(mailbox.Self)
                if not (List.contains (nodeRef.Path.Name.Split('_').[1] |> int) failedList ) then
                    system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(3.0),TimeSpan.FromSeconds(3.0),nodeRef ,DistributeKeys(mailbox.Self))
                // Console.WriteLine("SETSUCCESSORDEBUG " + successor.ToString() + "Predecessor " + mailbox.Self.ToString())
                // initialList <- List.append initialList [successor]
    
                              
            | SetPredecessor(nodeRef) ->
                predecessor <- nodeRef
                // Console.WriteLine ("Node" + mailbox.Self.ToString())
                // Console.WriteLine ("Predecessor" + predecessor.ToString())    
                
            | Stabilize(initialList) -> //Console.WriteLine("STDEBUG: "+ mailbox.Self.ToString())
                                        // Console.WriteLine("STDEBUGSUCC: "+ successor.ToString())
                                        successor <! RevertPredecessor(mailbox.Self, initialList)
                                        // Console.WriteLine("Stabilize invoked for: " + successor.ToString() + "By: " + mailbox.Self.Path.Name)
                              
            | RevertPredecessor(nodeRef, initialList) -> mailbox.Sender() <! StabilizeReceiver(predecessor, initialList)
                                                        //  Console.WriteLine("REVERT "+mailbox.Self.Path.Name + " Predecessor: " + predecessor.ToString())


            | StabilizeReceiver(nodeRef, initialList) -> let x = nodeRef.Path.Name.Split('_').[1] |> int
                                                         let succId = successor.Path.Name.Split('_').[1] |> int
                                                         
                                                         if (selfAddress > succId && x > selfAddress) || (selfAddress < succId && x > selfAddress && x < succId) then
                                                            // Console.WriteLine("Hello")
                                                            mailbox.Self <! SetSuccessor(nodeRef,initialList)
                                                            // nodeRef <! SetPredecessor(mailbox.Self)
                                                            // nodeRef <! Notify(mailbox.Self, nodeRef)
                                                            // Console.WriteLine("Stabilize Self: " + mailbox.Self.ToString() + " " + "Successor: " + nodeRef.ToString() )

                                                        //  else if flag = 0 && x > selfAddress && x < succId then
                                                        //     mailbox.Self <! SetSuccessor(nodeRef, "Old", initialList)
                                                        //     nodeRef <! SetPredecessor(mailbox.Self)
                                                        //     // nodeRef <! Notify(mailbox.Self, nodeRef)
                                                        //     Console.WriteLine("Stabilize Self: " + mailbox.Self.ToString() + " " + "Successor: " + nodeRef.ToString() )

                                                        //  else
                                                        //     Console.WriteLine("No action")

                                                            
                                                            // successor.Notify(mailbox.Self)

            | Notify(self, nodeRef) ->  let selfId = self.Path.Name.Split('_').[1] |> int
                                        let nodeRefId = nodeRef.Path.Name.Split('_').[1] |> int
                                        let predId = predecessor.Path.Name.Split('_').[1] |> int
                                        if isNull predecessor || (nodeRefId > predId && nodeRefId < selfId) then
                                            self <! SetPredecessor(nodeRef) 
                                                                
 
            | Lookup(keyHash,msg) ->
                if not (List.contains (mailbox.Self.Path.Name.Split('_').[1] |> int) failedList ) then
                    selfAddress <- mailbox.Self.Path.Name.Split("_").[1] |> int
                    // Console.WriteLine supervisorRef
                    // Console.WriteLine ("Source " + mailbox.Self.ToString())
                    // Console.WriteLine (selfAddress.ToString() + " " + fingerTable.ToString())
                    if keyHash > selfHash && keyHash < snd(fingerTable.[0]) then//if nodeRef = mailbox.Self then
                        // mailbox.Sender() <! LookupDone("Done")
                        if msg = "Store" then
                            //if not (List.contains keyHash keyList) then
                            keys <- List.append keys [keyHash]
                                //keyList <- List.append keyList [keyHash]
                            // Console.WriteLine ("Destination " + mailbox.Self.ToString() + " " + keys.ToString() + " " + keys.Length.ToString())

                            //supervisorRef <! LookupDone("Done")
                        else                        
                            supervisorRef <! LookupDone("Done")
                    // else if List.contains (nodeRef, sha1Hash nodeRef) fingerTable then
                    //     mailbox.Sender() <! LookupDone("")
                    else
                        let mutable low = ""
                        let mutable high = ""
                        let mutable tempBreak = false
                        let mutable i = 0
                        while not tempBreak && i < m - 1 do
                            low <- snd(fingerTable.[i]) //.Path.Name.Split("_").[1]
                            high <- snd(fingerTable.[i+1]) //.Path.Name.Split("_").[1]
                            if keyHash > low && keyHash < high then
                                // Console.WriteLine ("Forward to" + fst(fingerTable.[i]).ToString()) 
                                // mailbox.Sender() <! Forward(fst(fingerTable.[i]),keyHash)
                                if msg = "Store" then
                                    fst(fingerTable.[i]) <! Lookup (keyHash,"Store")
                                else
                                    supervisorRef <! Forward(fst(fingerTable.[i]),keyHash)
                                tempBreak <- true 
                            else 
                                i <- i + 1    

                        if not tempBreak  then//&& keyHash > snd(fingerTable.[m - 1]) then
                            if msg = "Store" then
                                if selfAddress = numNodes then
                                    successor <! StoreKey(keyHash)
                                else
                                    fst(fingerTable.[m - 1]) <! Lookup (keyHash,"Store")
                                // supervisorRef <! Forward(fst(fingerTable.[m - 1]),keyHash,"Store")
                            else
                                supervisorRef <! Forward(fst(fingerTable.[m - 1]),keyHash)
                    // Console.WriteLine flag        
                    // if flag = false then
                    //     // mailbox.Sender() <! Forward(fst(fingerTable.[m - 1]),keyHash)
                    //     supervisorRef <! Forward(fst(fingerTable.[m - 1]),keyHash)
                        // Console.WriteLine ("Forward to" + fst(fingerTable.[m - 1]).ToString()) 

            | StoreKey(keyHash) ->
                //if not (List.contains keyHash keyList) then
                keys <- List.append keys [keyHash]
                // Console.WriteLine ("Outlier Destination " + mailbox.Self.ToString() + " " + keys.ToString() + " " + keys.Length.ToString())
                // supervisorRef <! LookupDone("Done")

            | DistributeKeys(nodeRef) -> //Console.WriteLine("Dist: " + nodeRef.ToString())
                                        //  Console.WriteLine("Dist Self: " + mailbox.Self.ToString())
                                         let mutable sendKeys = []
                                         
                                        //  keys <- []
                                         let predecessorHash = sha1Hash (nodeRef.Path.Name.Split("_").[1])
                                         for currentKey in keys do
                                            if currentKey < predecessorHash then
                                                // Console.WriteLine("Hello")
                                                
                                                sendKeys <- List.append [currentKey] sendKeys
                                                keys <- keys |> List.indexed |> List.filter(fun(_,x)-> x <> currentKey) |> List.map snd
                                            // else 
                                            //     keys <- List.append [x] keys
                                         nodeRef <! ReceiveKeys(sendKeys)
                                        //  Console.WriteLine("DistKeys: " + mailbox.Self.Path.Name + "Keys: " + keys.ToString() )

            | ReceiveKeys(sendKeys) -> keys <- List.append sendKeys keys
                                    //    Console.WriteLine("Self: " + mailbox.Self.Path.Name + "Keys: " + keys.ToString())
                                               
            //        keyList <- List.append keyList [keyHash]
                    //Console.WriteLine ("Outlier Destination " + mailbox.Self.ToString() + " " + keys.ToString() + " " + keys.Length.ToString())
                // supervisorRef <! LookupDone("Done")

            | SendLookup(keyList) ->
                let mutable key = keyList .[rnd.Next(0,99)]
                while List.contains key keys do
                    key <- keyList .[rnd.Next(0,99)]
                if lookupCount < numRequests then
                    // Console.WriteLine (selfAddress.ToString() + " " + fingerTable.ToString() + " " + fingerTable.Length.ToString() + " " + m.ToString())
                    // Console.WriteLine keyList
                    let mutable tempBreak = false
                    let mutable i = 0
                    let mutable low = null
                    let mutable high = null

                    while i < m - 1 && not tempBreak do
                        low <- snd(fingerTable.[i]) //.Path.Name.Split("_").[1]
                        high <- snd(fingerTable.[i+1]) //.Path.Name.Split("_").[1]
                        if key > low && key < high then
                            // Console.WriteLine ("Forward to" + fst(fingerTable.[i]).ToString()) 
                            // mailbox.Sender() <! Forward(fst(fingerTable.[i]),keyHash)
                            fst(fingerTable.[i]) <! Lookup (key,"Lookup")
                            tempBreak <- true 
                        else 
                            i <- i + 1    

                    // if not tempBreak  then//&& keyHash > snd(fingerTable.[m - 1]) then
                    //     fst(fingerTable.[m - 1]) <! Lookup (key,"Lookup")

                    // initialList.[r.Next(0,initialList.Length - 1)],Lookup(sha1Hash (r.Next(0,(numNodes |> int)).ToString()),"Lookup"))
                    lookupCount <- lookupCount + 1
                // else if lookupCount = numRequests && not flag then
                //     supervisorRef <! RequestsDone ("Done")

            | Ping(_) -> mailbox.Sender() <! PingResponse("Ping")

            | PingResponse(msg) -> if msg = "Ping" then
                                       Console.WriteLine("Alive")
                                   else
                                       Console.WriteLine("Dead")

            | _ -> ignore()
               

            return! loop()
        }

    loop()

let master (mailbox: Actor<_>) =
    let mutable peersList = []
    let mutable failedList = []
    let mutable initialList = []
    let numNodes = numNodes |> int
    let mutable hops = 0.0
    let mutable lookups = 0.0
    // let mutable ring = Array.create (pown 2 m) null
    let rec loop() =
        actor {
            let! message = mailbox.Receive()
            match message with
            | Start(_) ->
                peersList <-
                    [ for i in 1 .. numNodes do
                        yield (spawn system ("Peer_" + string (i))) peer ]

                
                
                // Console.WriteLine(peersList.ToString())
                // peersList.[0] <! Initiate("Begin")
                // let mutable initialList = []
                let mutable tempList = []
                let rnd = Random()

                // if peersList.Length > 10 then
                //     let mutable count = 0
                //     let mutable tempInd = 0
                //     while count <= (peersList.Length/5 |> int) do
                //         tempInd <- rnd.Next(0,peersList.Length - 1)
                //         while List.contains peersList.[tempInd] initialList do
                //             tempInd <- rnd.Next(0,peersList.Length - 1)
                //         initialList <- List.append initialList [peersList.[tempInd]]
                //         tempList <- tempInd :: tempList
                //         count <- count + 1
                // else

                initialList <- peersList.[0..4]
                m <- Math.Ceiling(Math.Log(initialList.Length - failedList.Length |> float, 2.)) |> int
                // Console.WriteLine ("m " + m.ToString())
                // for i in initialList do
                //     Console.WriteLine(i)
                peersList
                |> List.iter (fun node ->
                if not (List.contains (node.Path.Name.Split('_').[1] |> int) failedList ) then
                        node
                        <! Initiate("Begin"))
                // peersList.[5] <! system.Terminate
                initialList
                |> List.iter (fun node ->
                        node
                        <! StaticInitiate(initialList))         

                // while initialList.Length < peersList.Length do
                // let mutable fin = ""
                // let mutable init = 0

                // while not (List.contains peersList.[init] initialList) do
                //     fin <- rnd.Next(5,peersList.Length - 1)
                //[rnd.Next(0, initialList.Length - 1)]
                // for i in 6..(numNodes - 1) do
                //     init <- rnd.Next(0,initialList.Length - 1)

                //     fin <- peersList.[i]
                //     initialList <- List.append initialList [fin]
                    // initialList.[init] <! FindSuccessor(fin,initialList)
                    // system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(4.0),initialList.[init] ,FindSuccessor(fin,initialList))
                // system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(3.0),initialList.[rnd.Next(0,initialList.Length - 1)] ,FindSuccessor(peersList.[rnd.Next(initialList.Length - 1,numNodes - 1)],initialList))



                // while fin = init || List.contains fin initialList do
                // fin <- peersList.[rnd.Next(5, peersList.Length - 1)]
                // fin2 <- peersList.[rnd.Next(0, peersList.Length - 1)]

                // let fin = null

                // Console.WriteLine ("Init " + init.ToString())
                //Console.WriteLine ("Fin " + fin.ToString())
                // Console.WriteLine ("Init2 " + init2.ToString())
                // Console.WriteLine ("Fin2 " + fin2.ToString())
                // init <- rnd.Next(0,initialList.Length - 1)
                // fin <- peersList.[7]
                // initialList <- List.append initialList [fin]
                
                // system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0),initialList.[init] ,FindSuccessor(fin,initialList))
                // fin <- peersList.[8]


                
                // system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0),TimeSpan.FromSeconds(3.0),peersList ,Stabilize(initialList))
                let mutable init = null
                let mutable newNode = null
                // let mutable i = 5
                // while i < numNodes do 
                
                for i in 5 .. numNodes - 1 do
                    let mutable initAdd = rnd.Next(0,numNodes - 1)
                    while not (List.contains initAdd failedList) do
                        initAdd <- rnd.Next(0,numNodes - 1)
                    init <- peersList.[initAdd - 1]
                    newNode <- peersList.[i]
                    // Console.WriteLine newNode
//                    initialList <- List.append initialList [newNode]
                    // let initAdd = init.Path.Name.Split('_').[1] |> int
                    let mutable newNodeAdd = i
                    while not (List.contains i failedList) do
                        newNodeAdd <- newNodeAdd + 1
                    newNode <- peersList.[newNodeAdd - 1]

                    
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0),init ,FindSuccessor(newNode,initialList))
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(4.0),peersList.[i - 1] ,Stabilize(initialList))
                    initialList <- List.append initialList [newNode]
                    tempList <- List.append tempList [newNode]
                    m <- Math.Ceiling(Math.Log(initialList.Length - failedList.Length |> float, 2.)) |> int
                    // i <-  i + initialList.Length
                    // Console.WriteLine m
//                    init <! FindSuccessor(newNode,initialList)
                //i <- 6
//                 while initialList.Length <= peersList.Length do
//                     init <- peersList.[rnd.Next(0,numNodes - 1)]
//                     while List.contains newNode initialList || List.contains newNode tempList || init = newNode do
//                         i <- rnd.Next(0,numNodes - 1)
//                         newNode <- peersList.[i]
//                     Console.WriteLine newNode
// //                    initialList <- List.append initialList [newNode]
//                     system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(6.0),init ,FindSuccessor(newNode,initialList))
//                     system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(10.0),peersList.[i - 1] ,Stabilize(initialList))
//                     initialList <- List.append initialList [newNode]
//                     // tempList <- List.append tempList [newNode]
//                     m <- Math.Ceiling(Math.Log(initialList.Length |> float, 2.)) |> int


                initialList
                |> List.iter (fun node ->
                        system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(5.0),TimeSpan.FromSeconds(3.0),node ,Stabilize(initialList)))

                initialList
                |> List.iter (fun node ->
                        system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(2.0),TimeSpan.FromSeconds(1.0),node ,StaticInitiate(initialList)))

                // peersList.[4] <! PoisonPill.Instance,
                let mutable key = ""
                let mutable keyList = []
                for i in 1..100 do
                    // while List.contains key keyList do
                    key <- sha1Hash (i.ToString()) //+ rnd.Next(1,numNodes).ToString()) // "Key_"  + 
                    keyList <- List.append keyList [key]
                                  
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(10.0),initialList.[rnd.Next(0,initialList.Length - 1)] ,Lookup(key,"Store"))
                // for k in keyList do
                //     Console.WriteLine k
                    //peersList.[i-1] <! Stabilize(initialList)

                //     // init <! FindSuccessor(newNode, initialList)
                //     system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(5.0),init ,FindSuccessor(newNode, initialList))
                //     system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(5.0),peersList.[i-1] ,Stabilize(initialList))

                initialList
                |> List.iter (fun node ->
                    system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(12.0),TimeSpan.FromSeconds(1.0),node ,SendLookup(keyList)))

                    // while fin = init || List.contains fin initialList do
                    //     let init = initialList.[rnd.Next(i, initialList.Length - 1)]
                    //     fin <- peersList.[rnd.Next(5, peersList.Length - 1)]
                
                //init <! FindSuccessor(fin,initialList)
                //mailbox.Self.  (4000) |> ignore
                // system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(3.0),peersList.[4] ,Stabilize(initialList))
                //peersList.[4] <! Stabilize(initialList)
                // initialList <- List.append initialList [fin2]

                // init2 <! FindSuccessor(fin2,initialList)
                    // initialList.[init] <! FindSuccessor(fin,initialList)
                    // initialList <- List.append initialList [fin]
                // for i in initialList do
                //     Console.WriteLine(i)   
                // Console.WriteLine("Newnode fingertable: " + fin) 
                // let testKey = "Song_3"
                // initialList.[4] <! Lookup (sha1Hash testKey)

//             | Join (_) ->

//                 let mutable init = null
//                 let mutable newNode = null
//                 // let mutable i = 5
//                 // while i < numNodes do 
//                 for i in 5 .. numNodes - 1 do
//                     init <- peersList.[rnd.Next(0,numNodes - 1)]
//                     newNode <- peersList.[i]
//                     // Console.WriteLine newNode
// //                    initialList <- List.append initialList [newNode]
//                     system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0),init ,FindSuccessor(newNode,initialList))
//                     system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(4.0),peersList.[i - 1] ,Stabilize(initialList))
//                     initialList <- List.append initialList [newNode]
//                     //tempList <- List.append tempList [newNode]
//                     m <- Math.Ceiling(Math.Log(initialList.Length |> float, 2.)) |> int

            | LookupDone(_) ->
                hops <- hops + 1.0
                lookups <- lookups + 1.0
                Console.WriteLine (lookups.ToString() + " " + ( hops/lookups |> float).ToString())
                if lookups = (numNodes*numRequests |> float)  then 
                    Console.WriteLine ("Total Hops " + hops.ToString())
                    Console.WriteLine ("Total Requests " + numRequests.ToString())
                    Console.WriteLine ("Total Lookups " + lookups.ToString())
                    Console.WriteLine ("Average Hops per lookup " + (hops/lookups).ToString())
                    system.WhenTerminated.Wait()

            | Forward(dest,nodeRef) ->
                hops <- hops + 1.0
                dest <! Lookup(nodeRef,"Lookup")

            | _ -> ignore()

            return! loop()
        }
    loop()

let masterActor = spawn system "master" master

masterActor <! Start("Start")
// system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(3.0), masterActor, Join ("Join"))
system.WhenTerminated.Wait()
