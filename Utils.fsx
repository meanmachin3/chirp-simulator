#load @"./Constants.fsx"

open System
open Constants.Constants
open System.Text.RegularExpressions

module TwitterEngine = 

    let searchFor (classifier: char) (tweet: string) = 
        tweet.Split(' ') |> Seq.filter(fun word -> word.[0] = classifier) |> Seq.map(fun word -> word.[1..]) |> Seq.toList

    let searchTweets (tweets:string[]) (key:string)=
        tweets |> Seq.filter (fun tweet -> tweet.IndexOf(key) > -1) |> Seq.toList
                
    let matchSample r m =
        let r = Regex(r)
        let m1 = r.Match m
        let idFound = m1.Groups.[1] |> string |> int
        idFound

    let stripchars chars str =
        Seq.fold
            (fun (str: string) chr ->
            str.Replace(chr |> Char.ToUpper |> string, "").Replace(chr |> Char.ToLower |> string, ""))
            str chars
