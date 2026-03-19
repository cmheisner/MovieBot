# MovieBot
Discord Movie Bot for scheduling, movie metadata collection, and organizing lists


manage the ⁠💼stash  lists with title/year or move the stash to a spreadsheet and manage that
send voting polls to ⁠🎟️general  and react with movie specific icons in order
update the ⁠🗓️schedule  after voting has closed and remove movies from the ⁠💼stash
take the latest from ⁠🗓️schedule and create an Event with the Apple TV title image
[1:48 AM]Comrade: This is from GPT: 
The bot should support this workflow:

1) Manage the #💼stash list
- The stash is a list of candidate movies.
- Each movie should at minimum have:
  - title
  - year
  - optional notes
  - optional Apple TV URL
  - optional image URL
  - added_by
  - added_at
  - status
- I’m open to either:
  A) keeping the stash in Discord and parsing messages in #💼stash
  B) moving the stash to a spreadsheet and managing it there
- I want you to recommend the better approach for reliability and simplicity, and explain why.
- If spreadsheet is chosen, use a provider pattern so storage can later be swapped.
- Design the stash system so duplicate title/year entries are prevented or flagged.

2) Send voting polls to #🎟️general
- The bot should pull a set of movies from the stash and create a voting poll in #🎟️general.
- The poll should list the movies in a clear ordered format.
- The bot should react with movie-specific icons in order.
- If custom movie icons are not available, fall back to numbered emoji in order.
- The bot should store enough metadata so it can later determine which poll corresponds to which stash entries.
- The poll should support a voting window and a “close voting” action.

3) Close voting and update #🗓️schedule
- After voting closes, the bot should determine the winning movie or winners.
- It should update the #🗓️schedule channel with the selected movie(s).
- It should remove selected movies from the stash or mark them as scheduled.
- It should handle ties in a defined way. Recommend a good tie-breaking rule.
- It should be idempotent so rerunning the close action does not duplicate schedule entries.

4) Create a Discord Event from the latest #🗓️schedule entry
- The bot should read the latest schedule entry from #🗓️schedule.
- It should create a Discord scheduled event from that entry.
- The event should use the Apple TV title image if available.
- If Apple TV image retrieval is unreliable or not directly automatable, design the system with:
  - a media metadata provider interface
  - manual override support for Apple TV URL and image URL
  - fallback behavior if no Apple TV image is available
- The event should include the movie title, year, start time, and any relevant description.
