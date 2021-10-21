#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Text

let sha1Hash input : string=
    let sha = new SHA1Managed()
    let hashB = sha.ComputeHash(Encoding.ASCII.GetBytes(input.ToString()))
    let hashS =
        hashB
        |> Array.map (fun (x: byte) -> String.Format("{0:X2}", x))
        |> String.concat String.Empty
    hashS

let ans = sha1Hash "Hello"
Console.WriteLine(ans)