import glob
import pandas as pd
from database.data_convert import process_shosenkyo_data, process_hireidaihyo_data


def all_data():
    shosenkyo_files = glob.glob(f"{"raw_data/shosenkyo"}/*.csv")
    hireidaihyo_files = glob.glob(f"{"raw_data/hireidaihyo"}/*.csv")

    all_districts = []
    all_municipalities = []
    all_candidates = []
    all_candidate_votes = []
    all_blocks = []
    all_prefectures = []
    all_parties = []
    all_party_votes = []

    for file_path in shosenkyo_files:
        df_districts, df_municipalities, df_candidates, df_candidate_votes = (
            process_shosenkyo_data(file_path)
        )
        all_districts.append(df_districts)
        all_municipalities.append(df_municipalities)
        all_candidates.append(df_candidates)
        all_candidate_votes.append(df_candidate_votes)

    for file_path in hireidaihyo_files:
        df_blocks, df_prefectures, df_parties, df_party_votes = (
            process_hireidaihyo_data(file_path)
        )
        all_blocks.append(df_blocks)
        all_prefectures.append(df_prefectures)
        all_parties.append(df_parties)
        all_party_votes.append(df_party_votes)

    # Concatenate all DataFrames of the same type
    merged_districts = pd.concat(
        all_districts,
        ignore_index=True,
    )
    merged_municipalities = pd.concat(
        all_municipalities,
        ignore_index=True,
    )
    merged_candidates = pd.concat(
        all_candidates,
        ignore_index=True,
    )
    merged_candidate_votes = pd.concat(
        all_candidate_votes,
        ignore_index=True,
    )
    merged_blocks = pd.concat(
        all_blocks,
        ignore_index=True,
    )
    merged_prefectures = pd.concat(
        all_prefectures,
        ignore_index=True,
    )
    merged_parties = pd.concat(
        all_parties,
        ignore_index=True,
    ).drop_duplicates()
    merged_party_votes = pd.concat(
        all_party_votes,
        ignore_index=True,
    )

    return (
        merged_districts,
        merged_municipalities,
        merged_candidates,
        merged_candidate_votes,
        merged_blocks,
        merged_prefectures,
        merged_parties,
        merged_party_votes,
    )


def seed_database(conn):
    cursor = conn.cursor()
    (
        df_districts,
        df_municipalities,
        df_candidates,
        df_candidate_votes,
        df_blocks,
        df_prefectures,
        df_parties,
        df_party_votes,
    ) = all_data()

    num_elect_data = {
        "北海道": 8,
        "東北": 13,
        "北関東": 19,
        "南関東": 22,
        "東京": 17,
        "北陸信越": 11,
        "東海": 21,
        "近畿": 28,
        "中国": 11,
        "四国": 6,
        "九州": 20,
        # more block data
    }

    # Insert blocks data
    for _, row in df_blocks.iterrows():
        block_name = row["block_name"]
        num_elect = num_elect_data.get(block_name, None)
        cursor.execute(
            "INSERT INTO Blocks (name, num_elect) VALUES (?, ?)",
            (block_name, num_elect),
        )
    conn.commit()

    # Insert prefectures data
    block_ids = {
        row["block_name"]: cursor.execute(
            "SELECT block_id FROM Blocks WHERE name = ?",
            (row["block_name"],),
        ).fetchone()[0]
        for _, row in df_blocks.iterrows()
    }
    for _, row in df_prefectures.iterrows():
        block_id = block_ids[row["block_name"]]
        cursor.execute(
            """
            INSERT INTO Prefectures (name, block_id)
            VALUES (?, ?)
            """,
            (
                row["prefecture_name"],
                block_id,
            ),
        )
    conn.commit()

    # Insert districts data
    prefecture_ids = {
        row["prefecture_name"]: cursor.execute(
            "SELECT prefecture_id FROM Prefectures WHERE name = ?",
            (row["prefecture_name"],),
        ).fetchone()[0]
        for _, row in df_prefectures.iterrows()
    }
    for _, row in df_districts.iterrows():
        prefecture_id = prefecture_ids[row["prefecture_name"]]
        cursor.execute(
            "INSERT INTO Districts (name, prefecture_id) VALUES (?, ?)",
            (
                row["district_name"],
                prefecture_id,
            ),
        )
    conn.commit()

    # Insert municipalities data
    district_ids = {
        row["district_name"]: cursor.execute(
            "SELECT district_id FROM Districts WHERE name = ?",
            (row["district_name"],),
        ).fetchone()[0]
        for _, row in df_districts.iterrows()
    }
    for _, row in df_municipalities.iterrows():
        district_id = district_ids[row["district_name"]]
        cursor.execute(
            """
            INSERT INTO Municipalities (name, district_id, num_voters, num_votes_cast, num_valid_votes)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                row["municipality_name"],
                district_id,
                row["num_voters"],
                row.get("num_votes_cast", 0),
                row.get("num_valid_votes", 0),
            ),
        )
    conn.commit()

    # Insert parties data
    for _, row in df_parties.iterrows():
        cursor.execute(
            "INSERT INTO Parties (name) VALUES (?)",
            (row["party_name"],),
        )
    cursor.execute(
        "INSERT INTO Parties (name) VALUES (?)",
        ("無",),
    )
    cursor.execute(
        "INSERT INTO Parties (name) VALUES (?)",
        ("諸派",),
    )
    conn.commit()

    # Insert candidates data
    party_ids = {
        row["party_name"]: cursor.execute(
            "SELECT party_id FROM Parties WHERE name = ?",
            (row["party_name"],),
        ).fetchone()[0]
        for _, row in df_parties.iterrows()
    }
    party_ids["無"] = cursor.execute(
        "SELECT party_id FROM Parties WHERE name = ?",
        ("無",),
    ).fetchone()[0]
    party_ids["諸派"] = cursor.execute(
        "SELECT party_id FROM Parties WHERE name = ?",
        ("諸派",),
    ).fetchone()[0]

    for _, row in df_candidates.iterrows():
        party_id = party_ids[row["party_name"]]
        district_id = district_ids[row["district_name"]]
        cursor.execute(
            """
            INSERT INTO Candidates (name, age, party_id, former_exp, overlap, district_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row["candidate_name"],
                row["age"],
                party_id,
                row["former"],
                row["overlap"],
                district_id,
            ),
        )

    conn.commit()

    # Insert shosenkyo votes data
    municipality_ids = {
        (row["municipality_name"], row["district_name"]): cursor.execute(
            """
            SELECT municipality_id FROM Municipalities WHERE name = ? AND district_id = (
                SELECT district_id FROM Districts WHERE name = ?
            )
            """,
            (row["municipality_name"], row["district_name"]),
        ).fetchone()[0]
        for _, row in df_municipalities.iterrows()
    }
    candidate_ids = {
        (row["candidate_name"], row["district_name"]): cursor.execute(
            """
            SELECT candidate_id 
            FROM Candidates 
            WHERE name = ? AND district_id = (
                SELECT district_id FROM Districts WHERE name = ?
            )
            """,
            (row["candidate_name"], row["district_name"]),
        ).fetchone()[0]
        for _, row in df_candidates.iterrows()
    }
    for _, row in df_candidate_votes.iterrows():
        municipality_id = municipality_ids[
            (row["municipality_name"], row["district_name"])
        ]
        candidate_id = candidate_ids[(row["candidate_name"], row["district_name"])]
        cursor.execute(
            """
            INSERT INTO Votes_Shosenkyo (municipality_id, candidate_id, votes)
            VALUES (?, ?, ?)
            """,
            (
                municipality_id,
                candidate_id,
                row["votes"],
            ),
        )

    conn.commit()

    # Insert hireidaihyo votes data
    prefecture_municipality_to_district = {
        (row["prefecture_name"], row["municipality_name"]): row["district_name"]
        for _, row in df_municipalities.iterrows()
    }
    for _, row in df_party_votes.iterrows():
        party_id = party_ids[row["party_name"]]
        district_name = prefecture_municipality_to_district[
            (row["prefecture_name"], row["municipality_name"])
        ]
        municipality_id = municipality_ids[(row["municipality_name"], district_name)]
        cursor.execute(
            """
            INSERT INTO Votes_Hireidaihyo (municipality_id, party_id, votes)
            VALUES (?, ?, ?)
            """,
            (
                municipality_id,
                party_id,
                row["votes"],
            ),
        )

    conn.commit()

    print("Database populated successfully!")
