# EMBRACE - <b><i>Emb</i></b>y <b><i>R</i></b>ecommendation <b><i>a</i></b>nd <b><i>C</i></b>ontext <b><i>E</i></b>ngine

EMBRACE is an intelligent and personalised recommendation engine for [Emby](https://emby.media/).

The aim of EMBRACE is to build a local, privacy-first recommendation engine that goes beyond basic recommendations based on simple metadata (genre overlap, similar actors/directors, etc).
Instead, the goal here is to support richer, more nuanced and intelligent, conversational recommendations.

## Motivation

Standard recommendation algorithms seen in popular streaming services can work well at times, but often succeed by _'throwing a bunch of recommendations at the wall and seeing what sticks'_. For example: "you watched a comedy movie, here are other comedy movies".

This may work fine at times, but **what if** we had a recommendation assistant who not only knew our watch history and our viewing habits, but one we could also converse with for better, more nuanced recommendations?

> A personal example: I found I'm quite a fan of 'dry Canadian humour' shows like _Trailer Park Boys_, >_Letterkenny_, and _Shoresy_. If you look up their tagged genres in IMdb, you'll find the following:
>
> 1.  **Trailer Park Boys**: _Mokumentary, Raunchy Comedy, Sitcom, Stoner Comedy, Comedy, Crime_
> 2.  **Letterkenny**: _Quick Comedy, Sitcom, Comedy_
> 3.  **Shoresy**: _Raunchy Comedy, Action, Comedy, Drama, Sport_
>
> If you fed just these genre tags into a recommendation algorithm, you might get some decent results among other less useful results. But what if I wanted to ask "_I really liked the sometimes subtle and implied humour in Trailer park Boys, mixed with the wacky 'trailer-trash' setting of the characters. Are there other shows that have a similar theme to this?_"

The idea is that EMBRACE would learn from **your** specific watch history, but it will also take it a step beyond that. It will analyse your watch habits per show/movie (e.g. how often do you actually finish watching the show
Did it take you multiple tries or did you binge watch it in a few days?). From there, it will also analyse each item in your Emby library over time to extract richer and more detailed sentiments about each show/movie, so ultimately you **can** ask for more specific and personalised recommendations, and also engage in conversational-style discussions to find your next great watch.

## Key Goals - Core focus for first MVP

### 1: Local and Private

The entire system will be built with a self-hosted-first approach. There will strictly be **NO** dependency or necessary requirements on external 3rd party internet API services that consume or otherwise 'see' your data.

There are planned future features to allow for cloud-based hosting of databases **if, and only if,** you specifically don't want to store your data locally (e.g. for storage redundancy or capacity reasons).

Any future integrations of 3rd party services (e.g. OpenAI, Claude, etc), if added, will be **strictly fully optional and will not retract from the functionality of EMBRACE if they are not used**. Any integration features like this will only be added in general if they can provide some real, worth-while benefit that can't be reasonably obtained on local consumer-grade hardware.

### 2: Nuanced Tagging and Classification

Library items will be processed and classified to provide rich and nuanced tags and extended metadata. Beyond basic genre classifications like "comedy", "romance", "action drama".

### 3: Conversational Interface

The main suggestion interface will be a chat-bot that can handle conversational-like questions and requests. Likely based on an LLM, it should allow a user to have discussions about recommendations or suggestions, while also accepting feedback or corrections from the user.

### 4. Modularity

While this may be a vague goal title, the idea of EMBRACE is to be modular in its integration, and its features:

-   it should be relatively simple for any Emby user to run an EMBRACE locally on their own hardware with minimal steps. This includes supporting varying degrees of hardware tiers - user's shouldn't need a compute farm just to run EMBRACE effectively, but also utilise powerful hardware if it's available.
-   It should work cleanly and efficiently for different users on a single Emby library/server. The whole point is a **personalised** service.
-   Features should be decoupled and modular where possible, not only for development purposes but also for user preferences. For example:
    -   If one part of the EMBRACE system is down, this shouldn't crash the whole system if it can be avoided
    -   If a user doesn't want the full chat-bot service, they shouldn't be forced to use it
    -   If a person's Emby set-up doesn't use Jellyseerr, then EMBRACE should still work fine for them
