open System
open System.Security.Cryptography

printfn "works"
let hash_string (input: string, algo: string) =
    let hash_bytes = input 
                        |> System.Text.Encoding.UTF8.GetBytes
                        |> HashAlgorithm.Create(algo).ComputeHash
    let hash_string = "0" + 
                        (hash_bytes
                        |> Seq.map (fun c -> c.ToString "x2")
                        |> Seq.reduce (+))
    hash_string

// let blah = "sjdhbsbd"

// let hash_type = "SHA1"
// let hash = hash_string (blah, hash_type)
// printfn "%s" hash
// let num =  bigint.Parse(hash, System.Globalization.NumberStyles.HexNumber)
// printfn "%O" num
// let backToHex = num.ToString "x2"
// printfn "%s" backToHex
// printfn "%i" backToHex.Length

let mutable num : bigint = bigint(2.0** 160.0) 
num <- (num - bigint(1.0))
let hash = num.ToString("X")
let num2 = bigint(2.0)
let hash0 = num2.ToString("x2")
let num3 =  bigint(2.0**54.0)
let mutable hash3 = num3.ToString("X")
let num4 = bigint(2.0**101.0)
let mutable hash4 = num4.ToString("X")
if hash4.Length < 41 then
    hash4 <- (String.replicate (41-hash4.Length) "0") + hash4

printfn "Length of %s is %i" hash3 hash.Length 
printfn "Length of %s is %i" hash4 hash4.Length 
printfn "%b" (hash3>hash4)

let next = (101-1) |>float
let power = 2.0 ** next
let addition = (num3 + bigint(2.0 ** next))

let finger_int_val =  (addition % num)
let mutable finger_val = finger_int_val.ToString("X")
printfn "%s %i" finger_val finger_val.Length