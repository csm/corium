#!/usr/bin/env python3
"""Generate `releases-1997.edn`, a curated MusicBrainz-shaped dataset of
notable 1997 releases for the corium browser demo.

The EDN shape matches `examples/musicbrainz/schema.edn` and the loader
convention used by `sample.edn`: each top-level vector is one atomic
transaction, applied in order, with entities linked by lookup refs on the
unique `:*/gid` attributes. Ordering is:

    artists + labels  ->  tracks  ->  media  ->  releases

DATA PROVENANCE
---------------
Artist, album, label and track *names* are real. Track *durations* are
approximate (rounded to a few seconds) and the UUID `gid`s are synthetic,
deterministic, and namespaced by type — they are NOT the canonical
MusicBrainz identifiers. Swap in a real extraction (same EDN shape) when a
network path to the MusicBrainz web service or data dumps is available.
"""

import sys

# gid namespace tags (first two hex digits) keep ids unique across types.
TAG = {"artist": 0xA1, "label": 0xB1, "track": 0xC1, "medium": 0xD1, "release": 0xE1}
_counters = {k: 0 for k in TAG}


def gid(kind: str) -> str:
    _counters[kind] += 1
    return f"{TAG[kind]:02x}{_counters[kind]:030x}"


def dur(m: int, s: int) -> int:
    return (m * 60 + s) * 1000


# Each album: release + its single artist + label + one CD medium + tracks.
# tracks: (title, duration_ms). Durations approximate.
ALBUMS = [
    {
        "release": "OK Computer",
        "type": "album", "status": "official", "country": "GB", "language": "eng",
        "artist": {"name": "Radiohead", "sort": "Radiohead", "type": "group",
                   "country": "GB", "start": 1985},
        "label": {"name": "Parlophone", "country": "GB"},
        "tracks": [
            ("Airbag", dur(4, 44)), ("Paranoid Android", dur(6, 23)),
            ("Subterranean Homesick Alien", dur(4, 27)),
            ("Exit Music (For a Film)", dur(4, 24)), ("Let Down", dur(4, 59)),
            ("Karma Police", dur(4, 21)), ("Fitter Happier", dur(1, 57)),
            ("Electioneering", dur(3, 50)), ("Climbing Up the Walls", dur(4, 45)),
            ("No Surprises", dur(3, 48)), ("Lucky", dur(4, 19)),
            ("The Tourist", dur(5, 24)),
        ],
    },
    {
        "release": "Homogenic",
        "type": "album", "status": "official", "country": "IS", "language": "eng",
        "artist": {"name": "Björk", "sort": "Björk", "type": "person",
                   "gender": "female", "country": "IS", "start": 1977},
        "label": {"name": "One Little Indian", "country": "GB"},
        "tracks": [
            ("Hunter", dur(4, 15)), ("Jóga", dur(5, 5)), ("Unravel", dur(3, 18)),
            ("Bachelorette", dur(5, 12)), ("All Neon Like", dur(5, 53)),
            ("5 Years", dur(4, 30)), ("Immature", dur(3, 8)),
            ("Alarm Call", dur(4, 20)), ("Pluto", dur(3, 32)),
            ("All Is Full of Love", dur(4, 30)),
        ],
    },
    {
        "release": "Homework",
        "type": "album", "status": "official", "country": "FR", "language": "eng",
        "artist": {"name": "Daft Punk", "sort": "Daft Punk", "type": "group",
                   "country": "FR", "start": 1993},
        "label": {"name": "Virgin", "country": "GB"},
        "tracks": [
            ("Daftendirekt", dur(2, 44)), ("WDPK 83.7 FM", dur(0, 28)),
            ("Revolution 909", dur(5, 26)), ("Da Funk", dur(5, 28)),
            ("Phœnix", dur(4, 55)), ("Fresh", dur(4, 3)),
            ("Around the World", dur(7, 7)), ("Rollin' & Scratchin'", dur(7, 27)),
            ("Teachers", dur(2, 53)), ("High Fidelity", dur(6, 0)),
            ("Rock'n Roll", dur(7, 32)), ("Oh Yeah", dur(2, 0)),
            ("Burnin'", dur(6, 54)), ("Indo Silver Club", dur(4, 32)),
            ("Alive", dur(5, 15)), ("Funk Ad", dur(0, 51)),
        ],
    },
    {
        "release": "The Fat of the Land",
        "type": "album", "status": "official", "country": "GB", "language": "eng",
        "artist": {"name": "The Prodigy", "sort": "Prodigy, The", "type": "group",
                   "country": "GB", "start": 1990},
        "label": {"name": "XL Recordings", "country": "GB"},
        "tracks": [
            ("Smack My Bitch Up", dur(5, 42)), ("Breathe", dur(5, 35)),
            ("Diesel Power", dur(4, 17)), ("Funky Shit", dur(5, 16)),
            ("Serial Thrilla", dur(5, 11)), ("Mindfields", dur(5, 40)),
            ("Narayan", dur(9, 5)), ("Firestarter", dur(4, 40)),
            ("Climbatize", dur(6, 38)), ("Fuel My Fire", dur(4, 19)),
        ],
    },
    {
        "release": "Urban Hymns",
        "type": "album", "status": "official", "country": "GB", "language": "eng",
        "artist": {"name": "The Verve", "sort": "Verve, The", "type": "group",
                   "country": "GB", "start": 1990},
        "label": {"name": "Hut", "country": "GB"},
        "tracks": [
            ("Bitter Sweet Symphony", dur(5, 58)), ("Sonnet", dur(4, 21)),
            ("The Rolling People", dur(7, 1)), ("The Drugs Don't Work", dur(5, 5)),
            ("Catching the Butterfly", dur(6, 26)), ("Neon Wilderness", dur(2, 37)),
            ("Space and Time", dur(5, 36)), ("Weeping Willow", dur(4, 49)),
            ("Lucky Man", dur(4, 53)), ("One Day", dur(5, 3)),
            ("This Time", dur(3, 50)), ("Velvet Morning", dur(4, 28)),
            ("Come On", dur(15, 15)),
        ],
    },
    {
        "release": "Be Here Now",
        "type": "album", "status": "official", "country": "GB", "language": "eng",
        "artist": {"name": "Oasis", "sort": "Oasis", "type": "group",
                   "country": "GB", "start": 1991},
        "label": {"name": "Creation Records", "country": "GB"},
        "tracks": [
            ("D'You Know What I Mean?", dur(7, 42)), ("My Big Mouth", dur(5, 2)),
            ("Magic Pie", dur(7, 19)), ("Stand by Me", dur(5, 56)),
            ("I Hope, I Think, I Know", dur(4, 22)), ("The Girl in the Dirty Shirt", dur(5, 49)),
            ("Fade In-Out", dur(6, 52)), ("Don't Go Away", dur(4, 48)),
            ("Be Here Now", dur(5, 13)), ("All Around the World", dur(9, 38)),
            ("It's Gettin' Better (Man!!)", dur(7, 0)),
            ("All Around the World (Reprise)", dur(2, 8)),
        ],
    },
    {
        "release": "The Colour and the Shape",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "Foo Fighters", "sort": "Foo Fighters", "type": "group",
                   "country": "US", "start": 1994},
        "label": {"name": "Roswell Records", "country": "US"},
        "tracks": [
            ("Doll", dur(1, 23)), ("Monkey Wrench", dur(3, 51)),
            ("Hey, Johnny Park!", dur(4, 8)), ("My Poor Brain", dur(3, 33)),
            ("Wind Up", dur(2, 32)), ("Up in Arms", dur(2, 15)),
            ("My Hero", dur(4, 20)), ("See You", dur(2, 27)),
            ("Enough Space", dur(2, 37)), ("February Stars", dur(4, 49)),
            ("Everlong", dur(4, 10)), ("Walking After You", dur(5, 3)),
            ("New Way Home", dur(5, 40)),
        ],
    },
    {
        "release": "Time Out of Mind",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "Bob Dylan", "sort": "Dylan, Bob", "type": "person",
                   "gender": "male", "country": "US", "start": 1961},
        "label": {"name": "Columbia Records", "country": "US"},
        "tracks": [
            ("Love Sick", dur(5, 21)), ("Dirt Road Blues", dur(3, 36)),
            ("Standing in the Doorway", dur(7, 43)), ("Million Miles", dur(5, 52)),
            ("Tryin' to Get to Heaven", dur(5, 21)), ("'Til I Fell in Love with You", dur(5, 17)),
            ("Not Dark Yet", dur(6, 29)), ("Cold Irons Bound", dur(7, 15)),
            ("Make You Feel My Love", dur(3, 32)), ("Can't Wait", dur(5, 47)),
            ("Highlands", dur(16, 31)),
        ],
    },
    {
        "release": "Either/Or",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "Elliott Smith", "sort": "Smith, Elliott", "type": "person",
                   "gender": "male", "country": "US", "start": 1991},
        "label": {"name": "Kill Rock Stars", "country": "US"},
        "tracks": [
            ("Speed Trials", dur(3, 0)), ("Alphabet Town", dur(3, 47)),
            ("Ballad of Big Nothing", dur(2, 42)), ("Between the Bars", dur(2, 21)),
            ("Pictures of Me", dur(3, 55)), ("No Name No. 5", dur(3, 27)),
            ("Rose Parade", dur(3, 26)), ("Punch and Judy", dur(2, 32)),
            ("Angeles", dur(2, 55)), ("Cupid's Trick", dur(3, 5)),
            ("2:45 AM", dur(3, 20)), ("Say Yes", dur(2, 19)),
        ],
    },
    {
        "release": "Portishead",
        "type": "album", "status": "official", "country": "GB", "language": "eng",
        "artist": {"name": "Portishead", "sort": "Portishead", "type": "group",
                   "country": "GB", "start": 1991},
        "label": {"name": "Go! Beat", "country": "GB"},
        "tracks": [
            ("Cowboys", dur(4, 38)), ("All Mine", dur(3, 51)),
            ("Undenied", dur(4, 20)), ("Half Day Closing", dur(3, 56)),
            ("Over", dur(4, 0)), ("Humming", dur(6, 1)),
            ("Mourning Air", dur(4, 12)), ("Seven Months", dur(4, 15)),
            ("Only You", dur(4, 58)), ("Elysium", dur(5, 54)),
            ("Western Eyes", dur(3, 59)),
        ],
    },
    {
        "release": "Ladies and Gentlemen We Are Floating in Space",
        "type": "album", "status": "official", "country": "GB", "language": "eng",
        "artist": {"name": "Spiritualized", "sort": "Spiritualized", "type": "group",
                   "country": "GB", "start": 1990},
        "label": {"name": "Dedicated", "country": "GB"},
        "tracks": [
            ("Ladies and Gentlemen We Are Floating in Space", dur(3, 27)),
            ("Come Together", dur(4, 58)), ("I Think I'm in Love", dur(8, 3)),
            ("All of My Thoughts", dur(4, 24)), ("Stay with Me", dur(4, 39)),
            ("Electricity", dur(3, 46)), ("Home of the Brave", dur(2, 33)),
            ("The Individual", dur(4, 25)), ("Broken Heart", dur(5, 8)),
            ("No God Only Religion", dur(3, 24)),
            ("Cool Waves", dur(5, 6)), ("Cop Shoot Cop...", dur(17, 14)),
        ],
    },
    {
        "release": "Dig Me Out",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "Sleater-Kinney", "sort": "Sleater-Kinney", "type": "group",
                   "country": "US", "start": 1994},
        "label": {"name": "Kill Rock Stars", "country": "US"},
        "tracks": [
            ("Dig Me Out", dur(2, 41)), ("One More Hour", dur(3, 56)),
            ("Turn It On", dur(2, 32)), ("The Drama You've Been Craving", dur(2, 14)),
            ("Heart Factory", dur(3, 2)), ("Words and Guitar", dur(2, 26)),
            ("It's Enough", dur(2, 4)), ("Little Babies", dur(2, 44)),
            ("Not What You Want", dur(3, 12)), ("Buy Her Candy", dur(2, 6)),
            ("Things You Say", dur(3, 20)), ("Dance Song '97", dur(2, 46)),
            ("Jenny", dur(3, 20)),
        ],
    },
    {
        "release": "The Lonesome Crowded West",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "Modest Mouse", "sort": "Modest Mouse", "type": "group",
                   "country": "US", "start": 1992},
        "label": {"name": "Up Records", "country": "US"},
        "tracks": [
            ("Teeth Like God's Shoeshine", dur(6, 22)), ("Heart Cooks Brain", dur(4, 6)),
            ("Convenient Parking", dur(4, 8)), ("Lounge (Closing Time)", dur(5, 60)),
            ("Jesus Christ Was an Only Child", dur(2, 6)), ("Doin' the Cockroach", dur(5, 47)),
            ("Cowboy Dan", dur(6, 12)), ("Trailer Trash", dur(5, 40)),
            ("Out of Gas", dur(3, 6)), ("Long Distance Drunk", dur(4, 46)),
            ("Shit Luck", dur(2, 55)), ("Truckers Atlas", dur(11, 4)),
            ("Polar Opposites", dur(3, 44)), ("Bankrupt on Selling", dur(3, 12)),
            ("Styrofoam Boots / It's All Nice on Ice, Alright", dur(6, 15)),
        ],
    },
    {
        "release": "I Can Hear the Heart Beating as One",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "Yo La Tengo", "sort": "Yo La Tengo", "type": "group",
                   "country": "US", "start": 1984},
        "label": {"name": "Matador", "country": "US"},
        "tracks": [
            ("Return to Hot Chicken", dur(1, 38)), ("Moby Octopad", dur(5, 15)),
            ("Sugarcube", dur(3, 18)), ("Damage", dur(4, 12)),
            ("Deeper Into Movies", dur(5, 11)), ("Shadows", dur(3, 24)),
            ("Stockholm Syndrome", dur(3, 2)), ("Autumn Sweater", dur(5, 20)),
            ("Little Honda", dur(3, 47)), ("Green Arrow", dur(5, 20)),
            ("One PM Again", dur(3, 21)), ("The Lie and How We Told It", dur(4, 16)),
            ("Center of Gravity", dur(2, 39)), ("Spec Bebop", dur(9, 45)),
            ("We're an American Band", dur(3, 33)), ("My Little Corner of the World", dur(2, 26)),
        ],
    },
    {
        "release": "Wu-Tang Forever",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "Wu-Tang Clan", "sort": "Wu-Tang Clan", "type": "group",
                   "country": "US", "start": 1992},
        "label": {"name": "Loud Records", "country": "US"},
        "tracks": [
            ("Reunited", dur(5, 20)), ("For Heavens Sake", dur(3, 51)),
            ("Cash Still Rules / Scary Hours", dur(3, 47)), ("Visionz", dur(3, 60)),
            ("As High as Wu-Tang Get", dur(3, 15)), ("Severe Punishment", dur(4, 60)),
            ("Older Gods", dur(4, 0)), ("Maria", dur(4, 44)),
            ("A Better Tomorrow", dur(4, 52)), ("It's Yourz", dur(3, 60)),
            ("Triumph", dur(5, 37)), ("Impossible", dur(5, 14)),
            ("Little Ghetto Boys", dur(4, 60)), ("Deadly Melody", dur(4, 12)),
            ("The City", dur(3, 40)),
        ],
    },
    {
        "release": "Life After Death",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "The Notorious B.I.G.", "sort": "Notorious B.I.G., The",
                   "type": "person", "gender": "male", "country": "US", "start": 1992},
        "label": {"name": "Bad Boy Records", "country": "US"},
        "tracks": [
            ("Somebody's Gotta Die", dur(4, 44)), ("Hypnotize", dur(3, 50)),
            ("Kick in the Door", dur(4, 48)), ("#!*@ You Tonight", dur(5, 8)),
            ("Last Day", dur(4, 21)), ("I Love the Dough", dur(4, 12)),
            ("What's Beef?", dur(5, 15)), ("Mo Money Mo Problems", dur(4, 16)),
            ("Nasty Boy", dur(4, 32)), ("Sky's the Limit", dur(4, 42)),
            ("Ten Crack Commandments", dur(3, 30)), ("Playa Hater", dur(3, 51)),
            ("Notorious Thugs", dur(5, 55)), ("Going Back to Cali", dur(4, 60)),
            ("Ni**as Bleed", dur(5, 3)), ("You're Nobody (Til Somebody Kills You)", dur(4, 56)),
        ],
    },
    {
        "release": "Baduizm",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "Erykah Badu", "sort": "Badu, Erykah", "type": "person",
                   "gender": "female", "country": "US", "start": 1994},
        "label": {"name": "Kedar Records", "country": "US"},
        "tracks": [
            ("Rimshot (Intro)", dur(1, 40)), ("On & On", dur(3, 47)),
            ("Appletree", dur(4, 42)), ("Otherside of the Game", dur(6, 20)),
            ("Sometimes...", dur(0, 38)), ("Next Lifetime", dur(6, 26)),
            ("Afro (Freestyle Skit)", dur(1, 10)), ("Certainly", dur(4, 46)),
            ("4 Leaf Clover", dur(4, 30)), ("No Love", dur(4, 30)),
            ("Drama", dur(5, 6)), ("Sometimes (Mix #9)", dur(3, 40)),
            ("Certainly (Flipped It)", dur(4, 12)), ("Rimshot (Outro)", dur(3, 15)),
        ],
    },
    {
        "release": "Supa Dupa Fly",
        "type": "album", "status": "official", "country": "US", "language": "eng",
        "artist": {"name": "Missy Elliott", "sort": "Elliott, Missy", "type": "person",
                   "gender": "female", "country": "US", "start": 1991},
        "label": {"name": "The Gold Mind", "country": "US"},
        "tracks": [
            ("Busta's Intro", dur(0, 30)), ("Hit 'Em wit da Hee", dur(4, 15)),
            ("Sock It 2 Me", dur(4, 13)), ("The Rain (Supa Dupa Fly)", dur(4, 12)),
            ("Beep Me 911", dur(3, 39)), ("They Don't Wanna #!*@", dur(4, 25)),
            ("Pass da Blunt", dur(3, 60)), ("Bite Our Style (Interlude)", dur(1, 20)),
            ("Friendly Skies", dur(4, 51)), ("Best Friends", dur(4, 22)),
            ("Don't Be Commin' (In My Face)", dur(4, 8)), ("Izzy Izzy Ahh", dur(3, 60)),
            ("Why You Hurt Me", dur(4, 20)), ("I'm Talkin'", dur(3, 46)),
            ("Gettaway", dur(4, 44)), ("Busta's Outro", dur(0, 40)),
        ],
    },
    {
        "release": "When I Was Born for the 7th Time",
        "type": "album", "status": "official", "country": "GB", "language": "eng",
        "artist": {"name": "Cornershop", "sort": "Cornershop", "type": "group",
                   "country": "GB", "start": 1991},
        "label": {"name": "Wiiija", "country": "GB"},
        "tracks": [
            ("Sleep on the Left Side", dur(4, 12)), ("Brimful of Asha", dur(5, 16)),
            ("Butter the Soul", dur(2, 20)), ("Chocolat", dur(3, 45)),
            ("We're in Yr Corner", dur(3, 12)), ("Funky Days Are Back Again", dur(3, 47)),
            ("What Is Happening?", dur(3, 60)), ("When the Light Appears Boy", dur(2, 44)),
            ("Coming Up", dur(3, 26)), ("Good Shit", dur(3, 12)),
            ("Good to Be on the Road Back Home", dur(4, 30)), ("It's Indian Tobacco My Friend", dur(4, 46)),
            ("Candyman", dur(3, 40)), ("State Troopers (Part 1)", dur(2, 30)),
            ("Norwegian Wood (This Bird Has Flown)", dur(4, 6)),
        ],
    },
    {
        "release": "Blur",
        "type": "album", "status": "official", "country": "GB", "language": "eng",
        "artist": {"name": "Blur", "sort": "Blur", "type": "group",
                   "country": "GB", "start": 1988},
        "label": {"name": "Food Records", "country": "GB"},
        "tracks": [
            ("Beetlebum", dur(5, 4)), ("Song 2", dur(2, 2)),
            ("Country Sad Ballad Man", dur(4, 26)), ("M.O.R.", dur(3, 27)),
            ("On Your Own", dur(4, 27)), ("Theme from Retro", dur(3, 37)),
            ("You're So Great", dur(3, 36)), ("Death of a Party", dur(4, 32)),
            ("Chinese Bombs", dur(1, 24)), ("I'm Just a Killer for Your Love", dur(4, 15)),
            ("Look Inside America", dur(4, 3)), ("Strange News from Another Star", dur(4, 1)),
            ("Movin' On", dur(3, 44)), ("Essex Dogs", dur(6, 12)),
        ],
    },
]


def esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def uuid_ref(attr: str, g: str) -> str:
    return f'[[:{attr}/gid #uuid "{g}"]]'


def main() -> None:
    out = []
    out.append(';; Curated MusicBrainz-shaped dataset: notable releases from 1997.')
    out.append(';; Generated by gen_releases_1997.py. Names are real; durations are')
    out.append(';; approximate and gids are synthetic (see the generator header).')
    out.append(';;')
    out.append(';; Transaction order: artists + labels -> tracks -> media -> releases.')
    out.append('')

    # Dedupe artists and labels by name; assign gids.
    artists = {}   # name -> (gid, meta)
    labels = {}    # name -> (gid, meta)
    for al in ALBUMS:
        a = al["artist"]
        if a["name"] not in artists:
            artists[a["name"]] = (gid("artist"), a)
        lb = al["label"]
        if lb["name"] not in labels:
            labels[lb["name"]] = (gid("label"), lb)

    # Assign track/medium/release gids per album (kept on the album dict).
    for al in ALBUMS:
        al["_tracks"] = [(gid("track"), t[0], t[1]) for t in al["tracks"]]
        al["_medium"] = gid("medium")
        al["_release"] = gid("release")

    # ── Transaction 1: artists + labels ──────────────────────────────────
    tx = ["[;; ── artists + labels ──"]
    for i, (name, (g, a)) in enumerate(artists.items(), start=1):
        lines = [f' {{:db/id "artist-{i}"',
                 f'  :artist/gid #uuid "{g}"',
                 f'  :artist/name "{esc(a["name"])}"',
                 f'  :artist/sortName "{esc(a["sort"])}"',
                 f'  :artist/type :artist.type/{a["type"]}']
        if "gender" in a:
            lines.append(f'  :artist/gender :artist.gender/{a["gender"]}')
        lines.append(f'  :artist/country :country/{a["country"]}')
        lines.append(f'  :artist/startYear {a["start"]}}}')
        tx.append("\n".join(lines))
    for i, (name, (g, lb)) in enumerate(labels.items(), start=1):
        lines = [f' {{:db/id "label-{i}"',
                 f'  :label/gid #uuid "{g}"',
                 f'  :label/name "{esc(lb["name"])}"',
                 f'  :label/type :label.type/originalProduction',
                 f'  :label/country :country/{lb["country"]}}}']
        tx.append("\n".join(lines))
    tx.append("]")
    out.append("\n".join(tx))
    out.append("")

    # ── Transaction 2: tracks ────────────────────────────────────────────
    tx = ["[;; ── tracks ──"]
    tnum = 0
    for al in ALBUMS:
        artist_gid = artists[al["artist"]["name"]][0]
        credit = al["artist"]["name"]
        for pos, (tg, title, d) in enumerate(al["_tracks"], start=1):
            tnum += 1
            lines = [f' {{:db/id "track-{tnum}"',
                     f'  :track/gid #uuid "{tg}"',
                     f'  :track/name "{esc(title)}"',
                     f'  :track/position {pos}',
                     f'  :track/duration {d}',
                     f'  :track/artists [[:artist/gid #uuid "{artist_gid}"]]',
                     f'  :track/artistCredit "{esc(credit)}"}}']
            tx.append("\n".join(lines))
    tx.append("]")
    out.append("\n".join(tx))
    out.append("")

    # ── Transaction 3: media ─────────────────────────────────────────────
    tx = ["[;; ── media ──"]
    for i, al in enumerate(ALBUMS, start=1):
        track_refs = " ".join(f'[:track/gid #uuid "{tg}"]'
                              for tg, _, _ in al["_tracks"])
        lines = [f' {{:db/id "medium-{i}"',
                 f'  :medium/gid #uuid "{al["_medium"]}"',
                 f'  :medium/format :medium.format/cd',
                 f'  :medium/position 1',
                 f'  :medium/trackCount {len(al["_tracks"])}',
                 f'  :medium/tracks [{track_refs}]}}']
        tx.append("\n".join(lines))
    tx.append("]")
    out.append("\n".join(tx))
    out.append("")

    # ── Transaction 4: releases ──────────────────────────────────────────
    tx = ["[;; ── releases ──"]
    for i, al in enumerate(ALBUMS, start=1):
        artist_gid = artists[al["artist"]["name"]][0]
        label_gid = labels[al["label"]["name"]][0]
        lines = [f' {{:db/id "release-{i}"',
                 f'  :release/gid #uuid "{al["_release"]}"',
                 f'  :release/name "{esc(al["release"])}"',
                 f'  :release/artists [[:artist/gid #uuid "{artist_gid}"]]',
                 f'  :release/artistCredit "{esc(al["artist"]["name"])}"',
                 f'  :release/status :release.status/{al["status"]}',
                 f'  :release/type :release.type/{al["type"]}',
                 f'  :release/country :country/{al["country"]}',
                 f'  :release/language :language/{al["language"]}',
                 f'  :release/year {al.get("year", 1997)}',
                 f'  :release/labels [[:label/gid #uuid "{label_gid}"]]',
                 f'  :release/media [[:medium/gid #uuid "{al["_medium"]}"]]}}']
        tx.append("\n".join(lines))
    tx.append("]")
    out.append("\n".join(tx))
    out.append("")

    text = "\n".join(out)
    n_tracks = sum(len(al["_tracks"]) for al in ALBUMS)
    sys.stderr.write(
        f"releases={len(ALBUMS)} artists={len(artists)} "
        f"labels={len(labels)} tracks={n_tracks}\n")
    sys.stdout.write(text)


if __name__ == "__main__":
    main()
