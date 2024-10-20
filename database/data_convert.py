import re
import pandas as pd


def process_shosenkyo_data(file_path):

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    districts = []
    municipalities = []
    candidates = []
    candidate_votes = []

    current_district = None
    current_municipalities = []

    for line in lines:
        # Match district name
        if "開票所名" not in line:
            district_match = re.match(r",,(.+?)([\d０-９]+区)", line)
            if district_match:
                current_district_prefecture = district_match.group(1)
                current_district_number = district_match.group(2)
                current_district = (
                    f"{current_district_prefecture}{current_district_number}"
                )
                districts.append(
                    {
                        "district_name": current_district,
                        "prefecture_name": current_district_prefecture,
                    }
                )
                continue

        # Match municipality name
        if "開票所名" in line:
            current_municipalities = re.split(r",+", line.strip())[3:]
            continue

        # Match voter counts, votes cast, valid votes
        if "有権者数" in line:
            num_voters = list(
                map(lambda x: round(float(x)), re.split(r",+", line.strip())[3:])
            )
            for i, municipality in enumerate(current_municipalities):
                municipalities.append(
                    {
                        "district_name": current_district,
                        "municipality_name": municipality,
                        "prefecture_name": current_district_prefecture,
                        "num_voters": num_voters[i],
                    }
                )
            continue

        if "投票者数" in line:
            num_votes_cast = list(
                map(lambda x: round(float(x)), re.split(r",+", line.strip())[3:])
            )
            for municipality_name, votes_cast in zip(
                current_municipalities, num_votes_cast
            ):
                for entry in municipalities:
                    if entry["municipality_name"] == municipality_name:
                        entry["num_votes_cast"] = votes_cast
                        break
            continue

        if "有効票" in line:
            num_valid_votes = list(
                map(lambda x: round(float(x)), re.split(r",+", line.strip())[3:])
            )
            for municipality_name, valid_votes in zip(
                current_municipalities, num_valid_votes
            ):
                for entry in municipalities:
                    if entry["municipality_name"] == municipality_name:
                        entry["num_valid_votes"] = valid_votes
                        break
            continue

        # Match candidate
        candidate_match = re.match(
            r"(当|),*(重複|),*(.+),(.+),(.+),(\d+)歳,(.+)",
            line,
        )
        if candidate_match:
            overlap = candidate_match.group(2).strip() == "重複"
            name = candidate_match.group(3).strip()
            party = candidate_match.group(4).strip()
            previous_experience = candidate_match.group(5).strip() == "前"
            age = int(candidate_match.group(6))

            candidates.append(
                {
                    "district_name": current_district,
                    "overlap": overlap,
                    "candidate_name": name,
                    "party_name": party,
                    "former": previous_experience,
                    "age": age,
                }
            )

            votes = list(
                map(
                    lambda x: round(float(x)),
                    re.split(r",+", candidate_match.group(7).strip())[1:],
                )
            )
            for i, municipality in enumerate(current_municipalities):
                candidate_votes.append(
                    {
                        "candidate_name": name,
                        "municipality_name": municipality,
                        "district_name": current_district,
                        "votes": votes[i],
                    }
                )

    df_districts = pd.DataFrame(districts)
    df_municipalities = pd.DataFrame(municipalities)
    df_candidates = pd.DataFrame(candidates)
    df_candidate_votes = pd.DataFrame(candidate_votes)

    return df_districts, df_municipalities, df_candidates, df_candidate_votes


def process_hireidaihyo_data(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    parties = []
    party_votes = []
    blocks = []
    prefectures = []
    current_block = None
    current_prefecture = None
    current_municipalities = []

    for line in lines:
        # Match prefecture names
        block_prefecture_match = re.match(
            r"(.+)ブロック,(.+)(県|都|府|道),比例票", line
        )
        if block_prefecture_match:
            current_block = block_prefecture_match.group(1)
            current_prefecture = block_prefecture_match.group(2)
            suffix = block_prefecture_match.group(3)
            if suffix == "道":
                current_prefecture += suffix
            blocks.append({"block_name": current_block})
            prefectures.append(
                {
                    "prefecture_name": current_prefecture,
                    "block_name": current_block,
                }
            )
            continue

        # Match municipality names
        if "政党名" in line:
            municipalities_raw = re.split(r",+", line.strip())[3:]
            current_municipalities = [
                m for m in municipalities_raw if m and "(%)" not in m
            ]
            continue

        # Match party name and votes
        columns = re.split(r",+", line.strip())
        if len(columns) > 3:
            party_name = columns[0]
            parties.append({"party_name": party_name})
            for i, municipality in enumerate(current_municipalities):
                vote_value = columns[2 * i + 3]
                if vote_value:
                    # Special case for this municipality (data was broken)
                    if municipality == "横浜市都筑区":
                        party_votes.append(
                            {
                                "party_name": party_name,
                                "municipality_name": "横浜市都筑区８区",
                                "prefecture_name": current_prefecture,
                                "votes": round(float(vote_value) / 2),
                            }
                        )
                        party_votes.append(
                            {
                                "party_name": party_name,
                                "municipality_name": "横浜市都筑区７区",
                                "prefecture_name": current_prefecture,
                                "votes": round(float(vote_value) / 2),
                            }
                        )
                    else:
                        party_votes.append(
                            {
                                "party_name": party_name,
                                "municipality_name": municipality,
                                "prefecture_name": current_prefecture,
                                "votes": round(float(vote_value)),
                            }
                        )

    df_blocks = pd.DataFrame(blocks).drop_duplicates()
    df_prefectures = pd.DataFrame(prefectures)
    df_parties = pd.DataFrame(parties).drop_duplicates()
    df_party_votes = pd.DataFrame(party_votes)

    return df_blocks, df_prefectures, df_parties, df_party_votes
