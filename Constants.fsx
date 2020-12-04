#r "nuget: Akka.FSharp"

open Akka.FSharp

module Constants =

    /// Log levels.
    let Error = 0
    let Warning = 1
    let Information = 2
    let Debug = 3

    /// Mentions and Hashtags
    let Mention = '@'
    let Hashtag = '#'


    let mutable PROCESSING = false

    let actions =
        [| "tweet"
           "subscribe"
           "retweet"
           "query" |]

    let queries =
        [| "searchTweets"
           "searchForHashtag"
           "searchForMention" |]

    let hashtags =
        [| "#dogoftheday"
           "#pet"
           "#pets"
           "#dogsofinstagram"
           "#ilovemydog"
           "#doggy"
           "#dog"
           "#cute"
           "#adorable"
           "#precious"
           "#nature"
           "#animal"
           "#animals"
           "#puppy"
           "#puppies"
           "#pup"
           "#petstagram"
           "#picpets"
           "#cutie"
           "#life"
           "#petsagram"
           "#tagblender"
           "#dogs"
           "#instagramdogs"
           "#dogstagram"
           "#ilovedog"
           "#ilovedogs"
           "#doglover"
           "#doglovers"
           "#tail" |]

    let search =
        [| "retweet"
           "donate"
           "religion"
           "discrimination"
           "everyone"
           "hating" |]

    let tweets =
        [| "If the Cleveland Cavaliers win the 2018 NBA finals I’ll buy everyone who retweet’s this a jersey"
           "broken. from the bottom of my heart, i am so so sorry. i don't have words."
           "We stand against racial discrimination. We condemn violence. You, I and we all have the right to be respected. We will stand together."
           "No one is born hating another person because of the color of his skin or his background or his religion..."
           "HELP ME PLEASE. A MAN NEEDS HIS NUGGIES"
           "Congratulations to the Astronauts that left Earth today. Good choice" 
           "We did it, JoeBiden"
           "Quarantine day 6."
           "For every retweet this gets, Pedigree will donate one bowl of dog food to dogs in need!"
           "Thank you for everything. My last ask is the same as my first. I'm asking you to believe—not in my ability to create change, but in yours." |]

    let userRegexMatch = "User([0-9]*)"
    let random = System.Random()
    let mutable totalTweetsToBeSent = 0
