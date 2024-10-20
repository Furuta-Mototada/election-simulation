-- Delete tables if exists
DROP TABLE IF EXISTS Blocks;
DROP TABLE IF EXISTS Prefectures;
DROP TABLE IF EXISTS Districts;
DROP TABLE IF EXISTS Municipalities;
DROP TABLE IF EXISTS Parties;
DROP TABLE IF EXISTS Candidates;
DROP TABLE IF EXISTS Votes_Shosenkyo;
DROP TABLE IF EXISTS Votes_Hireidaihyo;

-- Table for Blocks
CREATE TABLE Blocks (
    block_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    num_elect INTEGER NOT NULL  -- Number of representatives to be elected in this block
);

-- Table for Prefectures
CREATE TABLE Prefectures (
    prefecture_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    block_id INTEGER NOT NULL,  -- Foreign key to Blocks
    FOREIGN KEY (block_id) REFERENCES Blocks(block_id)
);

-- Table for Districts
CREATE TABLE Districts (
    district_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    prefecture_id INTEGER NOT NULL,  -- Foreign key to Prefectures
    FOREIGN KEY (prefecture_id) REFERENCES Prefectures(prefecture_id)
);

-- Table for Municipalities
CREATE TABLE Municipalities (
    municipality_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    district_id INTEGER NOT NULL,  -- Foreign key to Districts
    num_voters INTEGER NOT NULL,  -- Total number of eligible voters
    num_votes_cast INTEGER NOT NULL,  -- Total number of votes cast
    num_valid_votes INTEGER NOT NULL,  -- Total number of valid votes
    FOREIGN KEY (district_id) REFERENCES Districts(district_id)
);

-- Table for Parties
CREATE TABLE Parties (
    party_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

-- Table for Candidates
CREATE TABLE Candidates (
    candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    age INTEGER NOT NULL,
    party_id TEXT, -- Foreign key to Parties
    former_exp BOOLEAN NOT NULL,  -- True if candidate has been elected before
    overlap BOOLEAN NOT NULL,  -- True if candidate is also running for another election
    district_id INTEGER NOT NULL,  -- Foreign key to Districts
    FOREIGN KEY (district_id) REFERENCES Districts(district_id)
    FOREIGN KEY (party_id) REFERENCES Parties(party_id)
);

-- Table for Shosenkyo Votes
CREATE TABLE Votes_Shosenkyo (
    municipality_id INTEGER NOT NULL,
    candidate_id INTEGER NOT NULL,
    votes INTEGER NOT NULL,  -- Number of votes for the candidate in this municipality
    PRIMARY KEY (municipality_id, candidate_id),  -- Composite primary key
    FOREIGN KEY (municipality_id) REFERENCES Municipalities(municipality_id),
    FOREIGN KEY (candidate_id) REFERENCES Candidates(candidate_id)
);

-- Table for Hireidaihyo Votes
CREATE TABLE Votes_Hireidaihyo (
    municipality_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL,
    votes INTEGER NOT NULL,
    PRIMARY KEY (municipality_id, party_id),
    FOREIGN KEY (municipality_id) REFERENCES Municipalities(municipality_id),
    FOREIGN KEY (party_id) REFERENCES Parties(party_id)
);
