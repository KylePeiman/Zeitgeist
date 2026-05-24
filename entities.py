"""
entities.py — Seed entity list for Zeitgeist sentiment tracking.

Each entity has:
  - name: canonical name used for matching and display
  - aliases: alternative names/spellings to match in raw text
  - category: grouping for the dashboard
  - entity_type: person | brand | team | show | topic | product
"""

ENTITIES = [

    # ── ENTERTAINMENT — PEOPLE ─────────────────────────────
    {"name": "Taylor Swift",      "aliases": ["Taylor", "Swifties", "Swift"],           "category": "Entertainment", "entity_type": "person"},
    {"name": "Beyoncé",           "aliases": ["Beyonce", "Bey", "Queen Bey"],            "category": "Entertainment", "entity_type": "person"},
    {"name": "Kanye West",        "aliases": ["Ye", "Kanye"],                            "category": "Entertainment", "entity_type": "person"},
    {"name": "Drake",             "aliases": ["Drizzy", "Champagne Papi"],               "category": "Entertainment", "entity_type": "person"},
    {"name": "Eminem",            "aliases": ["Slim Shady", "Marshall Mathers"],         "category": "Entertainment", "entity_type": "person"},
    {"name": "Rihanna",           "aliases": ["RiRi", "Fenty"],                          "category": "Entertainment", "entity_type": "person"},
    {"name": "Lady Gaga",         "aliases": ["Gaga", "Mother Monster"],                 "category": "Entertainment", "entity_type": "person"},
    {"name": "Ariana Grande",     "aliases": ["Ari", "Ariana"],                          "category": "Entertainment", "entity_type": "person"},
    {"name": "Billie Eilish",     "aliases": ["Billie"],                                 "category": "Entertainment", "entity_type": "person"},
    {"name": "Doja Cat",          "aliases": ["Doja"],                                   "category": "Entertainment", "entity_type": "person"},
    {"name": "Kim Kardashian",    "aliases": ["Kim K", "Kardashian"],                    "category": "Entertainment", "entity_type": "person"},
    {"name": "MrBeast",           "aliases": ["Mr Beast", "Jimmy Donaldson"],            "category": "Entertainment", "entity_type": "person"},
    {"name": "Logan Paul",        "aliases": ["Logan"],                                  "category": "Entertainment", "entity_type": "person"},
    {"name": "Joe Rogan",         "aliases": ["Rogan", "JRE"],                           "category": "Entertainment", "entity_type": "person"},

    # ── POLITICS & BUSINESS ────────────────────────────────
    {"name": "Donald Trump",      "aliases": ["Trump", "MAGA", "45", "47"],              "category": "Politics", "entity_type": "person"},
    {"name": "Joe Biden",         "aliases": ["Biden", "46"],                            "category": "Politics", "entity_type": "person"},
    {"name": "Kamala Harris",     "aliases": ["Kamala", "Harris"],                       "category": "Politics", "entity_type": "person"},
    {"name": "AOC",               "aliases": ["Alexandria Ocasio-Cortez", "Ocasio"],     "category": "Politics", "entity_type": "person"},
    {"name": "Bernie Sanders",    "aliases": ["Bernie", "Sanders"],                      "category": "Politics", "entity_type": "person"},
    {"name": "Elon Musk",         "aliases": ["Musk", "Elon"],                           "category": "Politics", "entity_type": "person"},
    {"name": "Jeff Bezos",        "aliases": ["Bezos"],                                  "category": "Business", "entity_type": "person"},
    {"name": "Mark Zuckerberg",   "aliases": ["Zuckerberg", "Zuck"],                     "category": "Business", "entity_type": "person"},
    {"name": "Sam Altman",        "aliases": ["Altman"],                                 "category": "Business", "entity_type": "person"},
    {"name": "Tim Cook",          "aliases": ["Cook"],                                   "category": "Business", "entity_type": "person"},

    # ── SPORTS — PEOPLE ────────────────────────────────────
    {"name": "LeBron James",      "aliases": ["LeBron", "King James", "Bron"],           "category": "Sports", "entity_type": "person"},
    {"name": "Lionel Messi",      "aliases": ["Messi", "Leo"],                           "category": "Sports", "entity_type": "person"},
    {"name": "Cristiano Ronaldo", "aliases": ["Ronaldo", "CR7"],                         "category": "Sports", "entity_type": "person"},
    {"name": "Patrick Mahomes",   "aliases": ["Mahomes"],                                "category": "Sports", "entity_type": "person"},
    {"name": "Stephen Curry",     "aliases": ["Curry", "Steph"],                         "category": "Sports", "entity_type": "person"},
    {"name": "Caitlin Clark",     "aliases": ["Clark", "Caitlin"],                       "category": "Sports", "entity_type": "person"},
    {"name": "Novak Djokovic",    "aliases": ["Djokovic", "Novak", "Nole"],              "category": "Sports", "entity_type": "person"},
    {"name": "Serena Williams",   "aliases": ["Serena"],                                 "category": "Sports", "entity_type": "person"},
    {"name": "Tom Brady",         "aliases": ["Brady", "GOAT"],                          "category": "Sports", "entity_type": "person"},
    {"name": "Conor McGregor",    "aliases": ["McGregor", "Conor"],                      "category": "Sports", "entity_type": "person"},

    # ── SPORTS — TEAMS ─────────────────────────────────────
    {"name": "Dallas Cowboys",    "aliases": ["Cowboys", "America's Team"],              "category": "Sports", "entity_type": "team"},
    {"name": "New York Yankees",  "aliases": ["Yankees", "Yanks"],                       "category": "Sports", "entity_type": "team"},
    {"name": "Golden State Warriors", "aliases": ["Warriors", "GSW", "Dubs"],           "category": "Sports", "entity_type": "team"},
    {"name": "Manchester United", "aliases": ["Man United", "Man Utd", "MUFC"],          "category": "Sports", "entity_type": "team"},
    {"name": "Real Madrid",       "aliases": ["Madrid", "Los Blancos"],                  "category": "Sports", "entity_type": "team"},
    {"name": "Los Angeles Lakers","aliases": ["Lakers", "Lake Show"],                    "category": "Sports", "entity_type": "team"},
    {"name": "Barcelona",         "aliases": ["Barca", "FCB", "Blaugrana"],              "category": "Sports", "entity_type": "team"},
    {"name": "New England Patriots", "aliases": ["Patriots", "Pats"],                   "category": "Sports", "entity_type": "team"},

    # ── COMPANIES & BRANDS ─────────────────────────────────
    {"name": "Apple",             "aliases": ["Apple Inc", "AAPL"],                      "category": "Tech", "entity_type": "brand"},
    {"name": "Google",            "aliases": ["Alphabet", "GOOG"],                       "category": "Tech", "entity_type": "brand"},
    {"name": "Amazon",            "aliases": ["AWS", "AMZN"],                            "category": "Tech", "entity_type": "brand"},
    {"name": "Netflix",           "aliases": ["NFLX"],                                   "category": "Tech", "entity_type": "brand"},
    {"name": "Tesla",             "aliases": ["TSLA"],                                   "category": "Tech", "entity_type": "brand"},
    {"name": "OpenAI",            "aliases": ["Open AI"],                                "category": "Tech", "entity_type": "brand"},
    {"name": "Meta",              "aliases": ["Facebook", "Instagram", "WhatsApp"],      "category": "Tech", "entity_type": "brand"},
    {"name": "Microsoft",         "aliases": ["MSFT", "MS"],                             "category": "Tech", "entity_type": "brand"},
    {"name": "Disney",            "aliases": ["Walt Disney", "DIS"],                     "category": "Entertainment", "entity_type": "brand"},
    {"name": "TikTok",            "aliases": ["Tik Tok", "ByteDance"],                   "category": "Tech", "entity_type": "brand"},
    {"name": "Twitter/X",         "aliases": ["Twitter", "X.com", "X"],                  "category": "Tech", "entity_type": "brand"},
    {"name": "Uber",              "aliases": ["Uber Eats"],                              "category": "Tech", "entity_type": "brand"},
    {"name": "Spotify",           "aliases": ["SPOT"],                                   "category": "Tech", "entity_type": "brand"},
    {"name": "Nike",              "aliases": ["NKE", "Just Do It"],                      "category": "Brands", "entity_type": "brand"},
    {"name": "McDonald's",        "aliases": ["McDonalds", "Mickey D's", "Golden Arches"], "category": "Brands", "entity_type": "brand"},

    # ── SHOWS & MOVIES ─────────────────────────────────────
    {"name": "Game of Thrones",   "aliases": ["GOT", "GoT"],                             "category": "Entertainment", "entity_type": "show"},
    {"name": "Stranger Things",   "aliases": ["Stranger Things Netflix"],                 "category": "Entertainment", "entity_type": "show"},
    {"name": "The Last of Us",    "aliases": ["TLOU", "Last of Us"],                     "category": "Entertainment", "entity_type": "show"},
    {"name": "House of the Dragon", "aliases": ["HOTD", "HotD"],                        "category": "Entertainment", "entity_type": "show"},
    {"name": "Breaking Bad",      "aliases": ["BB", "Walter White"],                     "category": "Entertainment", "entity_type": "show"},
    {"name": "The Bear",          "aliases": ["Bear FX"],                                "category": "Entertainment", "entity_type": "show"},
    {"name": "Succession",        "aliases": ["Succession HBO"],                          "category": "Entertainment", "entity_type": "show"},
    {"name": "Squid Game",        "aliases": ["Squid Game Netflix"],                     "category": "Entertainment", "entity_type": "show"},
    {"name": "Marvel",            "aliases": ["MCU", "Marvel Cinematic Universe"],        "category": "Entertainment", "entity_type": "brand"},
    {"name": "Star Wars",         "aliases": ["StarWars", "The Force"],                  "category": "Entertainment", "entity_type": "brand"},

    # ── AI & TECH PRODUCTS ─────────────────────────────────
    {"name": "ChatGPT",           "aliases": ["GPT-4", "GPT4", "GPT"],                  "category": "AI", "entity_type": "product"},
    {"name": "Claude",            "aliases": ["Claude AI", "Anthropic"],                 "category": "AI", "entity_type": "product"},
    {"name": "Gemini",            "aliases": ["Google Gemini", "Bard"],                  "category": "AI", "entity_type": "product"},
    {"name": "GitHub Copilot",    "aliases": ["Copilot", "CoPilot"],                     "category": "AI", "entity_type": "product"},
    {"name": "iPhone",            "aliases": ["iOS", "Apple iPhone"],                    "category": "Tech", "entity_type": "product"},
    {"name": "Cybertruck",        "aliases": ["Tesla Cybertruck"],                       "category": "Tech", "entity_type": "product"},
    {"name": "Vision Pro",        "aliases": ["Apple Vision Pro", "visionOS"],           "category": "Tech", "entity_type": "product"},

    # ── MUSIC & ALBUMS ─────────────────────────────────────
    {"name": "The Tortured Poets Department", "aliases": ["TTPD", "Tortured Poets"],    "category": "Music", "entity_type": "product"},
    {"name": "GNX",               "aliases": ["Kendrick GNX"],                           "category": "Music", "entity_type": "product"},
    {"name": "Cowboy Carter",     "aliases": ["Beyonce Cowboy Carter", "Act II"],        "category": "Music", "entity_type": "product"},
    {"name": "Chromakopia",       "aliases": ["Tyler Chromakopia"],                      "category": "Music", "entity_type": "product"},
    {"name": "Short n Sweet",     "aliases": ["Ariana Short n Sweet"],                   "category": "Music", "entity_type": "product"},

    # ── GAMES ──────────────────────────────────────────────
    {"name": "Fortnite",          "aliases": ["Fortnite Battle Royale"],                 "category": "Gaming", "entity_type": "product"},
    {"name": "Call of Duty",      "aliases": ["COD", "Warzone"],                         "category": "Gaming", "entity_type": "product"},
    {"name": "Minecraft",         "aliases": ["MC"],                                     "category": "Gaming", "entity_type": "product"},
    {"name": "GTA",               "aliases": ["Grand Theft Auto", "GTA 6", "GTA VI"],   "category": "Gaming", "entity_type": "product"},
    {"name": "Elden Ring",        "aliases": ["EldenRing"],                              "category": "Gaming", "entity_type": "product"},
    {"name": "Baldur's Gate",     "aliases": ["BG3", "Baldurs Gate 3"],                  "category": "Gaming", "entity_type": "product"},
    {"name": "League of Legends", "aliases": ["LoL", "League"],                          "category": "Gaming", "entity_type": "product"},
    {"name": "Valorant",          "aliases": ["Val"],                                    "category": "Gaming", "entity_type": "product"},
    {"name": "Roblox",            "aliases": ["RBLX"],                                   "category": "Gaming", "entity_type": "product"},

    # ── TOPICS ─────────────────────────────────────────────
    {"name": "Artificial Intelligence", "aliases": ["AI", "Machine Learning", "ML"],    "category": "Topics", "entity_type": "topic"},
    {"name": "Climate Change",    "aliases": ["Global Warming", "Climate Crisis"],       "category": "Topics", "entity_type": "topic"},
    {"name": "Cryptocurrency",    "aliases": ["Crypto", "Web3"],                         "category": "Topics", "entity_type": "topic"},
    {"name": "Bitcoin",           "aliases": ["BTC", "Satoshi"],                         "category": "Topics", "entity_type": "topic"},
    {"name": "Remote Work",       "aliases": ["WFH", "Work From Home", "Hybrid Work"],  "category": "Topics", "entity_type": "topic"},
    {"name": "Cancel Culture",    "aliases": ["Cancelled", "Call Out Culture"],          "category": "Topics", "entity_type": "topic"},
]

# Quick lookup: alias → canonical entity name
ALIAS_MAP = {}
for entity in ENTITIES:
    ALIAS_MAP[entity["name"].lower()] = entity["name"]
    for alias in entity["aliases"]:
        ALIAS_MAP[alias.lower()] = entity["name"]

ENTITY_NAMES = [e["name"] for e in ENTITIES]
CATEGORIES = list(set(e["category"] for e in ENTITIES))
